# 
# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# BASIC AIRFLOW IMPORTS TO RUN THE DAGS
# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable

# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Calling the import functions created by MSE DX
# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
import airflow_library as bot_func
import connections_airflow as dfos_con
import connections_airflow_write as dfos_con_write

import json
import requests



def workflow_status_api(form_id, base_url, status):         
    """
    Function to update the workflow status of a form by making a POST request to the API.

    Parameters:
        form_id (int): The unique identifier of the form whose status needs to be updated.
        base_url (str): The base URL of the API environment (e.g., 'https://dfos.co' for production).
        status (int): The new workflow status to be assigned to the form.
                      Status codes:
                      - 0: Pending
                      - 1: Approved
                      - 2: Rejected
                      - 3: Draft
                      - 4: Sent for Approval
                      - 5: Pending at Approver 1
                      - 6: Pending at Approver 2
                      - 7: Master File
                      - 8: Hidden

    Returns:
        None
    """
    
    # Construct the full API endpoint URL
    url = base_url + '/api/v3/workflow'
    
    # Prepare the JSON payload to be sent in the request body
    payload = {
        "formId": form_id,
        "status": status
    }
    
    # Debugging log: Print the payload before making the API request
    print("Payload:", payload)

    response = None  # Initialize response variable

    try:
        # Send the HTTP POST request to update workflow status
        response = requests.post(url, json=payload)  # Using `json=payload` to send data as JSON
        
        # Debugging log: Print the response text from the server
        print("Response Text:", response.text)
        
        # Check the HTTP response status code
        if response.status_code == 200:
            print("Data posted successfully")  # Successful update
        else:
            print(f"Failed to post data. Status Code: {response.status_code}")  # Handle API failure
        
    except requests.exceptions.RequestException as e:
        # Handle any request-related errors (e.g., network issues, API downtime)
        print(f"Error occurred in function 'workflow_status_api': {e}")

    finally:
        # Final block always executes, useful for cleanup or logging purposes
        print("Execution of workflow_status_api is complete.")


def fetch_linked_main_form(connection, form_id):
    try:
        cur = connection.cursor()

        query = '''
            SELECT forms.linked_main_form
            FROM form_via_form_main_forms AS forms
            WHERE forms.fvf_main_form_id = %s;
'''

        cur.execute(query, (form_id))
        result = cur.fetchall()

        if result[0][0] == None:
            return form_id
        else: 
            return result[0][0]

    except Exception as e:
        print("Error occured in the function :- ", e)
        raise


# **kwargs allows you to pass a variable number of keyword arguments to a function. 
# These arguments are collected into a dictionary where the keys are the argument names and the values are the corresponding values passed.
# This is useful when you want to handle named arguments dynamically, without knowing them beforehand.
def main_code(**kwargs):    

# ----------------------------------------------------------------------------------------------------------------------------------------------------------------
# Extracting the values sent from the config.... DO NOT CHANGE THIS AT ALL

    params = kwargs.get('dag_run').conf


    # Extract values from the config
    audit_id = params['audit_id']                          # Audit ID related to the submission
    form_id = params['form_id']                            # Current form ID
    workflow_status = params['workflow_status']            #
    url = params["url"]                                    


    # Optional: Extract next_form_id from nested JSON string inside 'conf' key (if present)
    # This structure is passed by the configurator inside a 'conf' key as a stringified JSON.
    if 'conf' in params and params['conf']:
        try:
            conf = json.loads(params['conf'])  # Convert stringified JSON to Python object
            if isinstance(conf, list) and conf and 'next_form_id' in conf[0]:
                next_form_id = conf[0]['next_form_id']  # ID of the next form to be assigned
        except json.JSONDecodeError:
            # JSON was not formatted correctly - ignoring safely
            pass
    # ----------------------------------------------------------------------------------------------------------------------------------------------------------------
    # Print/log key variable values for debugging and auditing purposes
    # Helps trace issues if something fails during execution

    print(f"::group:: PARAMS =====")

    print("Audit Id:- ", audit_id)
    print("Form Id:-  ", form_id)
    print("Workflow Status:- ", workflow_status)
    print("URL:- ", url)

    print("::endgroup::")


    # ----------------------------------------------------------------------------------------------------------------------------------------------------------------
    # Setting up email subject and database connection
    # Note: Push notification logic is not implemented at this point

    connection = dfos_con.connection_for_all_replica(url)  
    # Read-only DB connection — uses the correct replica based on the given URL

# ----------------------------------------------------------------------------------------------------------------------------------------------------------------
 
    print("-------------------------------------------------------------------------------------------------------------------")

    linked_main_form = fetch_linked_main_form(connection, form_id)
    print("Linked Main form:- ", linked_main_form)
    
    workflow_status_api(form_id, url, workflow_status)

# ----------------------------------------------------------------------------------------------------------------------------------------------------------------
# CHANGE THESE THINGS...
# Establishing database connection (read-only for QA environment)

# ----------------------------------------------------------------------------------------------------------------------------------------------------------------
 

    connection.close()
    return "DesignX"


# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# The following code is a mandatory structure for defining an Airflow DAG. It contains essential configurations for Airflow to recognize and manage this workflow.
# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

default_args={
    'owner': 'OWNER',                                   # Write your username here
    'retries': 0,                                               # For the time being, the code does not need any retry attempts and hence is set to 0.
    # 'start_date' : datetime.datetime(2024, 7, 7, 17, 11),     # If the code is set to scheduling, then based on the airflow's configuration set it to UTC or IST. Make sure the you write current time.
}

with DAG(
    dag_id='CompanyName_ProjectName_GlobalStandardBot_workflow_approval_status',                      # This should be unique across all dags on that VM. Make sure to follow the dag nomenclature mentioned in the ppt.
    default_args=default_args,
    # schedule_interval='* * * * *',                            # If the code needs to run on a schedule/cron, it is defined here. In this case, it means run every minute
    schedule = None,                                            # If there is no scheduling
    catchup=False,                                              # It ensures that the DAG does not backfill missed runs between the start_date and the current date. Only the latest scheduled run will be executed.
    max_active_runs=1,                                          # Ensures only one active run at a time
    concurrency=1,                                              # Only one task instance can run concurrently
    tags = ["CompanyName", "ProjectName", "Type of DAG", "OWNER"],
    default_view='graph',) as dag:                        
        
        
        send_mail = PythonOperator(
            dag=dag,
            task_id="task_1",
            python_callable=main_code                           # It specifies the Python function to be executed by this task.  
        )

send_mail