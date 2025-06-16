# If approver:-  1 means approve and 2 means reject
# 
# 
# 
# https://airflowprod.dfos.co:8080/api/v1/dags/CompanyName_ProjectName_GlobalStandardBot_schedule_form/dagRun
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
from datetime import datetime, timedelta


def fetch_approver(connection, audit_id, approver_field_id):
    try:
        cur = connection.cursor()

        query = '''
            SELECT ans.fvf_main_field_type
            FROM form_via_form_main_audit_answers AS ans
            WHERE ans.fvf_main_audit_id = %s and ans.fvf_main_field_id= %s
        '''

        cur.execute(query, (audit_id,approver_field_id))
        result = cur.fetchall()

        if result[0][0].lower() == "radio":
            print("Type of Field is : - ", result[0][0])
            
            query = '''
            SELECT ans.answer_option_value
            FROM form_via_form_main_audit_answers AS ans
            WHERE ans.fvf_main_audit_id = %s and ans.fvf_main_field_id= %s
        '''
            cur.execute(query, (audit_id,approver_field_id))
            result = cur.fetchall()

            return result[0][0]
        else:
            
            print("Type of Field is : - ", result[0][0])
            
            query = '''
            SELECT op.fvf_main_field_option_value
            FROM form_via_form_main_audit_answers AS ans
            JOIN form_via_form_main_field_options AS op ON op.fvf_main_field_option_id = ans.fvf_main_field_option_id
            WHERE ans.fvf_main_audit_id = %s and ans.fvf_main_field_id= %s
        '''
            cur.execute(query, (audit_id,approver_field_id))
            result = cur.fetchall()

            return result[0][0]

    except Exception as e:
        print("Error occured in the func fetch_approver:- ", e)
        raise

def fetch_value_id(connection, audit_id, module_field_id):
    try:
        cur = connection.cursor()

        query = '''
            SELECT ans.fvf_main_field_type
            FROM form_via_form_main_audit_answers AS ans
            WHERE ans.fvf_main_audit_id = %s and ans.fvf_main_field_id= %s
        '''

        cur.execute(query, (audit_id,module_field_id))
        result = cur.fetchall()

        if result[0][0].lower() == "radio":
            print("Type of Field is : - ", result[0][0])
            
            query = '''
            SELECT ans.answer_option_value
            FROM form_via_form_main_audit_answers AS ans
            WHERE ans.fvf_main_audit_id = %s and ans.fvf_main_field_id= %s
        '''
            cur.execute(query, (audit_id,module_field_id))
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
            cur.execute(query, (audit_id,module_field_id))
            result = cur.fetchall()
        elif result[0][0].lower() == "api":
            print("Type of Field is : - ", result[0][0])
            
            query = '''
            SELECT op.fvf_main_field_option_id
            FROM form_via_form_main_audit_answers AS ans
            WHERE ans.fvf_main_audit_id = %s and ans.fvf_main_field_id= %s
        '''
            cur.execute(query, (audit_id,module_field_id))
            result = cur.fetchall()
        elif result[0][0].lower() == "user":
            print("Type of Field is : - ", result[0][0])
            
            query = '''
            SELECT ans.fvf_main_field_option_id
            FROM form_via_form_main_audit_answers AS ans
            WHERE ans.fvf_main_audit_id = %s and ans.fvf_main_field_id= %s
        '''
            cur.execute(query, (audit_id,module_field_id))
            result = cur.fetchall()
        else:
            print("The module is not found in either Dropdown, API or Radio and hence will fail below!!!")

        return result[0][0]
    except Exception as e:
        print("Error occured in the function fetch_value_id :- ", e)
        raise


def fetch_date(connection,audit_id, date_field_id):
    try:
        
        cur = connection.cursor()

        query = '''
            SELECT ans.answer
            FROM form_via_form_main_audit_answers AS ans
            WHERE ans.fvf_main_audit_id = %s and ans.fvf_main_field_id= %s
        '''

        cur.execute(query, (audit_id,date_field_id))
        result = cur.fetchall()

        return result[0][0]

    except Exception as e:
        print("Error occured in the function fetch_date:- ", e)
        raise

def fetch_from_audits(connection, audit_id):
    try:
        cur = connection.cursor()
        sql_query = '''          
                            SELECT ans.module_id as your_name_dag_id, ans.linked_mainaudit_id
                            FROM form_via_form_main_audits AS ans
                            WHERE ans.fvf_main_audit_id = %s;
                            '''
        cur.execute(sql_query, (audit_id))
        result = cur.fetchall()

        return result[0][1]

    except Exception as e:
        print(f"Error occurred in the func => fetch_from_audits: {e}")
        raise  

def fetch_compnay_id(connection, module_id):
    try:
        cur = connection.cursor()
        sql_query = '''          
                            SELECT mo.company_id
                            FROM modules AS mo
                            WHERE mo.module_id = %s
                            '''
            # print(sql_query)
        cur.execute(sql_query, (module_id))
        result = cur.fetchall()

        return result[0][0]

    except Exception as e:
        print(f"Error occurred in the func => fetch_compnay_id: {e}")
        raise  

def get_answers_by_field_names(connection, audit_id, field_ids):
    """
    Returns a dictionary of {field_name: answer} for given audit_id and list of field_ids.
    
    Parameters:
    - cur: PyMySQL cursor object.
    - audit_id: integer.
    - field_ids: list of integers.
    
    Returns:
    - dict: {field_name: answer}
    """
    try:
        cur = connection.cursor()
        if not field_ids:
            return {}
        
        print("Field ids:- ", field_ids)

        placeholders = ', '.join(['%s'] * len(field_ids))
        query = f"""
        SELECT ans.answer, ans.fvf_main_field_name
        FROM form_via_form_main_audit_answers AS ans
        WHERE ans.fvf_main_audit_id = {audit_id} AND ans.fvf_main_field_id IN ({field_ids});
        """

        cur.execute(query)
        results = cur.fetchall()
        print("results:- ", results)

        # PyMySQL with default cursor returns tuples in the same order as selected
        return {field_name: answer for answer, field_name in results}
    except Exception as e:
        print("Error occured in the function get_answers_by_field_names:- ", e)
        raise

def url_call(base_url, bot_user_id, module_id, form_id, schedule_user, company_id, start_date, end_date, linked_main_audit_id,schedule_json):
    
    try:
        entry = {
            'schedule_by': str(bot_user_id),
            'Module_id': str(module_id),
            'FORM_ID': str(form_id),
            'scheduled_User': str(schedule_user),
            'Audit_Type': "1",
            'Company_ID': str(company_id),
            'audit date': str(start_date),
            'endDate': str(end_date),
            'Frequency': "Daily",
            'AllowOnce': "1",
            'reference_audit_id': linked_main_audit_id,
            "schedule_json_data":str(schedule_json),
            'schedule_description':"",
 
        }

        print("Posting data to URL")

        url = f"{base_url}api/v3/schedule"
        headers = {'Content-Type': 'application/json'}

        print("API end_point:- ", url)
        print("FORM DATA:- ", json.dumps(entry, indent=4))

        response = requests.post(url, data=json.dumps(entry), headers=headers)

        print("Response Text:", response.text)
        
        if response.status_code == 200:
            print("Data posted successfully")
            print(f"Response Text:- ", response.text)
        else:
            print(f"Failed to post data. Status Code: {response.status_code}")
            print(f"Response Text:- ", response.text)
        return response
    
    except Exception as e:
        print(f"Error occurred while posting data: {e}")
        raise

def schedule_interval_update(base_url,table, column_name, value, cond_field, cond_value, schedule_id, audit_id, token,user_id):
    try:
        url = f"{base_url}dfos/api/v3/update_data"

        params = {
        "table": str(table),
        "field": str(column_name),
        "value": str(value),
        "condField": str(cond_field),
        "condValue": str(cond_value),
        "user_id":user_id
        }
 
        headers = {
        "Authorization": f"{token}"
        }
        print("Sending request to:", url)
        print("With parameters:", params)
 
        response = requests.get(url, params=params, headers=headers)
        print("Response status code:", response.status_code)
 
        response.raise_for_status()
        print("Response JSON:", response.json())
        return response.json()
    except requests.exceptions.RequestException as e:
        print("API request failed:", e)
        return None

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
    module_field_id = params['module_field_id']                          # ID of the main field (used for answers/messages)
    module_id = params['module_id']    # Field ID related to a file attachment
    form_field_id = params['form_field_id']                        # Name of the form
    form_id_schedule = params['form_id_schedule']                        # Name of the form
    schedule_field_users = params['schedule_field_users']                        # Name of the form
    schedule_static_users = params['schedule_static_users']                        # Name of the form
    bot_user_id = params['bot_user_id']                        # Name of the form
    start_date_field_id = params['start_date_field_id']                        # Name of the form
    start_date = params['start_date']                        # Name of the form
    end_date_field_id = params['end_date_field_id']                        # Name of the form
    end_date = params['end_date']                        # Name of the form
    url = params["url"]                                    # URL to be used for sending or referencing further data
    approver_field_id = params['approver_field_id']
    card_details = params['card_details']


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
    print("Field Id where I will find the module Id:- ", module_field_id)
    print("Module Id:- ", module_id)
    print("Field Id where i will find the form id:- ", form_field_id)
    print("Form ID which needs to be shceduled:- ", form_id_schedule)
    print("Schedule Field Users:- ", schedule_field_users)
    print("Schedule Static Users:- ", schedule_static_users)
    print("Bot User ID:- ", bot_user_id)
    print("Start Dtae Field Id:- ", start_date_field_id)
    print("Start Date in Date Format:- ", start_date)
    print("End Date Field ID:- ", end_date_field_id)
    print("End Date (= no of days+ Start Date):- ", end_date)
    print("URL:- ", url)
    print("Approver Field Id:- ", approver_field_id)
    print("card_details:- ", card_details)

    print("::endgroup::")


    # ----------------------------------------------------------------------------------------------------------------------------------------------------------------
    # Setting up email subject and database connection
    # Note: Push notification logic is not implemented at this point

    connection = dfos_con.connection_for_all_replica(url)  
    # Read-only DB connection — uses the correct replica based on the given URL

# ----------------------------------------------------------------------------------------------------------------------------------------------------------------
 
    print("-------------------------------------------------------------------------------------------------------------------")
    if approver_field_id == "":
        pass
    else:
        approver_check = fetch_approver(connection,audit_id, approver_field_id)

        if approver_check == 2:
            print("The form has been rejected and hence there will be no SCHEDULING")
            return "DesignX"
        else:    
            audit_id = fetch_from_audits(connection, audit_id)
            print("Now the audit id is:- ", audit_id)

    
    if module_field_id != "":
        module_id = fetch_value_id(connection, audit_id, module_field_id)
    print("Module Id :- ", module_id)

    if form_field_id != "":
        form_id_schedule = fetch_value_id(connection, audit_id, form_field_id)
    print("Form Id which needs to be scheduled:- ", form_id_schedule)

    if schedule_field_users != "":
        users = fetch_value_id(connection, audit_id, schedule_field_users)

    if schedule_static_users != "":
        schedule_static_users = schedule_static_users +","+users
    else:
        schedule_static_users = users

    print("Users for which the forms needs to be scheduled:- ", schedule_static_users)

    if start_date_field_id != "":
        start_date = fetch_date(connection,audit_id, start_date_field_id)
    print("Start Date :- ", start_date)

    if end_date != "":
        try:
            end_date_value = int(end_date)
            end_date_calc = (datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=end_date_value)).date()
            print("End Date (= no of days + Start Date):- ", end_date_value)
            end_date = str(end_date_calc)
        except Exception as e:
            print("Error calculating end date:", e)
    elif end_date_field_id != "":
        end_date = fetch_date(connection,audit_id, end_date_field_id)
    
    print("End Date :- ", end_date)

    linked_main_audit_id = fetch_from_audits(connection, audit_id)
    
    # If there's no linked main audit (i.e., value is 0), default to the current audit ID
    if linked_main_audit_id == 0:
        linked_main_audit_id = audit_id
    print("Linked Main Audit ID:", linked_main_audit_id)

    company_id = fetch_compnay_id(connection, module_id)
    print("Company Id:- ", company_id)

    schedule_json = get_answers_by_field_names(connection, audit_id, card_details)
    schedule_json = json.dumps(schedule_json)
    print("Schedule Json which will be shwon on the card:- ", schedule_json)
    


    response = url_call(url, bot_user_id, module_id, form_id_schedule, schedule_static_users, company_id, start_date, end_date, linked_main_audit_id, schedule_json)
    print(response)

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
    dag_id='CompanyName_ProjectName_GlobalStandardBot_schedule_form',                      # This should be unique across all dags on that VM. Make sure to follow the dag nomenclature mentioned in the ppt.
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