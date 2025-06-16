#####
# --------------------------------------------------------------------------------
# DAG TASK:
#       Retirgger the MAIN NC FORM when the NC is rejected anywhere.
# --------------------------------------------------------------------------------

# ----------------------
# PRE-REQUISITES by DFOS-DEVELOPER:
# ----------------------
# 1) DFOS Developer must map universal tags.
#    For now, global tags must match Excel column headers.
# 2) Form field types must be TEXTBOX or NUMBER.
#    Avoid Dropdowns, APIs, or any unsupported types.

# ----------------------
# HOW TO ATTACH THE BOT:
# ----------------------
#       1) BOT NAME: Any display name for DFOS
#       2) BOT URL Format: 
#           https://airflowprod.dfos.co:8080/api/v1/dags/{dag_id}/dagRuns
#               Example:  https://airflowprod.dfos.co:8080/api/v1/dags/CompanyName_ProjectName_GlobalStandardBot_NC_retrigger_when_rejected/dagRuns
#       3) BOT CONF (JSON Format):
#           { "submission_form_id": "913", "bot_user_id": "236" }
#               
#               - submission_form_id: ID of the form to be submitted
#               - bot_user_id: Create bot user in Digital Twin, log in once, and change password if new

# ----------------------
# REQUIREMENTS:
# ----------------------
# 1) Master form fields should be globally tagged to match Excel headers
# 2) Must have the following:
#    - Master Form ID
#    - Bot User ID
#    - File path: /home/airflowdfos/attachments/{dag_name}/yourfile.xlsx
# ----------------------

# ---------------------------------------------------------------------------
# WORKING:
#       1) This DAG receives an Excel file via form submission, downloads it, and reads its contents.
#       2) Each row in the Excel represents a form submission.
#       3) For each row:
#           - The row is mapped to fields in a target form using global tags.
#           - A dynamic JSON is generated based on the field structure.
#           - The form is submitted via API using a bot user.
#       4) After all rows are processed, the file is deleted and the DB connection is closed.
# ---------------------------------------------------------------------------

# ----------------------
# STANDARD CONVENTIONS:
# ----------------------
#       1) In each SQL query, select the primary ID column as NAME_DAG_id
#       2) Add tags at the end of the DAG script:
#           tags = ["CompanyName", "ProjectName", "Type of DAG", "OWNER"]
#       3) Set the Airflow owner to your username and include it in the tags
# --------------------------------------------------------------------------------

# -------------------------------------
# Standard Airflow and Python Libraries
# -------------------------------------

from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator  # For custom Python logic and branching
from airflow.operators.bash import BashOperator  # For shell command execution
from datetime import datetime, timedelta, time, date  # For DAG scheduling and time management
from airflow.models import Variable  # To retrieve stored Airflow variables

# -------------------------------------
# Commonly used Python Libraries
# -------------------------------------

import requests       # To send HTTP requests (API calls)
import json           # To create, parse JSON objects (used in payloads)

# -------------------------------------
# Custom Internal Libraries
# -------------------------------------

import connections_airflow as dfos_con                # For DB read connections
import airflow_library as bot_func                    # For common bot-related utilities
import connections_airflow_write as dfos_con_write    # For DB write connections

# --------------------------------------------------------------------------------
# AUTHORSHIP
# Created by: Aviral Tanwar
# Copyright: DesignX DFOS
# --------------------------------------------------------------------------------

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------FUNCTIONS WHICH WILL BE CALLED BY THE MAIN CODE IS WRITTEN BELOW--------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def fetch_value_id(connection, audit_id, field_id):
    try:
        cur = connection.cursor()

        query = '''
            SELECT ans.fvf_main_field_type
            FROM form_via_form_main_audit_answers AS ans
            WHERE ans.fvf_main_audit_id = %s and ans.fvf_main_field_id= %s
        '''

        cur.execute(query, (audit_id,field_id))
        result = cur.fetchall()

        if result[0][0].lower() == "radio":
            print("Type of Field is : - ", result[0][0])
            
            query = '''
            SELECT ans.answer_option_value
            FROM form_via_form_main_audit_answers AS ans
            WHERE ans.fvf_main_audit_id = %s and ans.fvf_main_field_id= %s
        '''
            cur.execute(query, (audit_id,field_id))
            result = cur.fetchall()

            return result[0][0]
        elif result[0][0].lower() == "dropdown":
            print("Type of Field is : - ", result[0][0])
            
            query = '''
            SELECT op.fvf_main_field_option_value
            FROM form_via_form_main_audit_answers AS ans
            JOIN form_via_form_main_field_options AS op ON op.fvf_main_field_option_id = ans.fvf_main_field_option_id
            WHERE ans.fvf_main_audit_id = %s and ans.fvf_main_field_id= %s
        '''
            cur.execute(query, (audit_id,field_id))
            result = cur.fetchall()
        elif result[0][0].lower() == "api":
            print("Type of Field is : - ", result[0][0])
            
            query = '''
            SELECT op.fvf_main_field_option_id
            FROM form_via_form_main_audit_answers AS ans
            WHERE ans.fvf_main_audit_id = %s and ans.fvf_main_field_id= %s
        '''
            cur.execute(query, (audit_id,field_id))
            result = cur.fetchall()
        elif result[0][0].lower() == "user":
            print("Type of Field is : - ", result[0][0])
            
            query = '''
            SELECT ans.fvf_main_field_option_id
            FROM form_via_form_main_audit_answers AS ans
            WHERE ans.fvf_main_audit_id = %s and ans.fvf_main_field_id= %s
        '''
            cur.execute(query, (audit_id,field_id))
            result = cur.fetchall()
        else:
            print("The module is not found in either Dropdown, API or Radio and hence will fail below!!!")

        return result[0][0]
    except Exception as e:
        print("Error occured in the function fetch_value_id :- ", e)
        raise

def task_status_api(nc_id, user_id, base_url, auth):
    try:
        payload = {
                "nc_id": f"{nc_id}",
                "user_id": f"{user_id}"
            }
        
        url = f"{base_url}api/v3/changeTaskStatus"

        headers = {'Authorization': f'{auth}'}

        print("Posting data to URL")

        print("Payload:- ", payload)
        print("API :- ", url)

        response = requests.post(url, data=payload, headers = headers)
        print("Response Text:", response.text)

        if response.status_code == 200:
            print("Data posted successfully")
            print(f"Response Text:- ", response.text)
        else:
            print(f"Failed to post data. Status Code: {response.status_code}")
            print(f"Response Text:- ", response.text)
        return response

    except Exception as e:
        print(f"Error occurred in the func => task_status_api: {e}")
        raise 

def users_func(connection, user_id):
    try:
        cur = connection.cursor()

        sql_query = '''
                    SELECT us.Authorization, us.email
                    FROM users AS us
                    WHERE us.user_id = %s
                    AND us.deleted_at = "0000-00-00 00:00:00";
                        '''

        cur.execute(sql_query, (user_id))  # Passing audit_id as a parameter
        result = cur.fetchall()
        return result[0][0], result[0][1]
        
    except Exception as e:
        print(f"Error occurred in the func => users_func: {e}")
        raise  

# **kwargs allows you to pass a variable number of keyword arguments to a function. 
# These arguments are collected into a dictionary where the keys are the argument names and the values are the corresponding values passed.
# This is useful when you want to handle named arguments dynamically, without knowing them beforehand.
def main_code(**kwargs):   

    # Access the config dictionary from the DAG run context
    params = kwargs.get('dag_run').conf

    # Extract values from the config
    audit_id =  params['audit_id']                              
    form_id = params['form_id'] 
    url = params['url'] 
    nc_id = params['nc_id'] 
    field_id = params['field_id'] 
    bot_user_id = params['bot_user_id']

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

    print("FORM ID:- ",form_id)
    print("AUDIT ID:- ", audit_id)
    print("NEXT FORM ID:- ", next_form_id)
    print("URL:- ", url)
    print("BOT USER ID:- ", bot_user_id)
    print("Field ID Mapping:- ", field_id)

    print("::endgroup::")

    # ----------------------------------------------------------------------------------------------------------------------------------------------------------------
    # Setting up email subject and database connection
    # Note: Push notification logic is not implemented at this point

    connection = dfos_con.connection_for_all_replica(url)  
    # Read-only DB connection — uses the correct replica based on the given URL

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------MAIN CODE'S LOGIC STARTS FROM BELOW------------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    print("-------------------------------------------------------------------------------------------------------------------")
    print("-------------------------------------------------------------------------------------------------------------------")

    flag = fetch_value_id(connection, audit_id, field_id)
    print("ACCEPTED OR REJECTED:- ", flag)

    if flag != 2:
        print("Either the Audit has been accepted or other than rejected value has been chosen")
        connection.close()
        return "DesignX"
    
    
    authorization, email_next = users_func(connection, bot_user_id)
    print("authorization:- ", authorization, "email_next:- ", email_next)
    
    response = task_status_api(nc_id, bot_user_id, url, authorization)
    print("Response:- ", response)


    return "DesignX"



# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# The following code is a mandatory structure for defining an Airflow DAG. It contains essential configurations for Airflow to recognize and manage this workflow.
# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
default_args={
    'owner': 'Aviral_Tanwar',
    'retries': 0,
}

with DAG(
    dag_id='CompanyName_ProjectName_GlobalStandardBot_NC_retrigger_when_rejected',                      # This should be unique across all dags on that VM. Make sure to follow the dag nomenclature mentioned in the ppt.
    default_args=default_args,
    # schedule_interval='* * * * *',                            # If the code needs to run on a schedule/cron, it is defined here. In this case, it means run every minute
    schedule = None,                                            # If there is no scheduling
    catchup=False,                                              # It ensures that the DAG does not backfill missed runs between the start_date and the current date. Only the latest scheduled run will be executed.
    max_active_runs=1,                                          # Ensures only one active run at a time
    concurrency=1,                                              # Only one task instance can run concurrently
    tags = ["Productized_Code", "form_submission_bot", "Aviral_Tanwar"],
    default_view='graph',) as dag:                        
        
        data_fetch = PythonOperator(
            dag=dag,
            task_id="task_1",
            python_callable=main_code
        )

data_fetch