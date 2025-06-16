from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime,timedelta,time,date
from airflow.models import Variable

import requests
import json
import os
import pandas as pd
import subprocess
import logging
import copy
import numpy as np

import connections_airflow as dfos_con
import airflow_library as bot_func
import connections_airflow_write as dfos_con_write


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

        return result[0][0],result[0][1]

    except Exception as e:
        print(f"Error occurred in the func => fetch_from_audits: {e}")
        raise  

def fetch_from_audits_form_id(connection, audit_id):
    try:
        cur = connection.cursor()
        sql_query = '''          
                            SELECT ans.fvf_main_form_id
                            FROM form_via_form_main_audits AS ans
                            WHERE ans.fvf_main_audit_id = %s;
                            '''
        cur.execute(sql_query, (audit_id))
        result = cur.fetchall()

        return result[0][0],result[0][1]

    except Exception as e:
        print(f"Error occurred in the func => fetch_from_audits: {e}")
        raise  


    
def form_status_update(base_url, form_id, form_status, user_id, token):
    try:
        url = f"{base_url}api/v3/formStatusUpdate"
 
        payload = {
        "form_id": form_id,
        "form_status": form_status,
        "user_id":user_id
        }
 
        files=[

]   
        headers = {
  'Authorization': token,
  'Cookie': 'ci_session=5pgjcr5lmnuj72ah68tthdg31rvui0u4'
}
 

        response = requests.post(url, data=payload, headers=headers)

        print("Response Text:", response.text)
        
        if response.status_code == 200:
            print("Data posted successfully")
            print(f"Response Text:- ", response.text)
        else:
            print(f"Failed to post data. Status Code: {response.status_code}")
            print(f"Response Text:- ", response.text)
        return response
    
    except requests.exceptions.RequestException as e:
        print("API request failed:", e)
        return None



def users_func(connection, user_id):
    try:
        cur = connection.cursor()

        sql_query = f'''
                    SELECT us.Authorization, us.email
                    FROM users AS us
                    WHERE us.user_id = {user_id}
                    AND us.deleted_at = "0000-00-00 00:00:00";
                        '''

        cur.execute(sql_query)  # Passing audit_id as a parameter
        result = cur.fetchall()
        return result[0][0], result[0][1]
        
    except Exception as e:
        print(f"Error occurred in the func => users_func: {e}")
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
    bot_user_id = params['bot_user_id']   
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
    print("bot_user_id:- ", bot_user_id)
    print("URL:- ", url)
    print("Approver Field Id:- ", approver_field_id)
    print("card_details:- ", card_details)
    print("next_form_id:- ",next_form_id)

    print("::endgroup::")


    # ----------------------------------------------------------------------------------------------------------------------------------------------------------------
    # Setting up email subject and database connection
    # Note: Push notification logic is not implemented at this point

    connection = dfos_con.connection_for_all_replica(url)  
    # Read-only DB connection — uses the correct replica based on the given URL

# ----------------------------------------------------------------------------------------------------------------------------------------------------------------
 
    print("-------------------------------------------------------------------------------------------------------------------")

    module_id, linked_main_audit_id = fetch_from_audits(connection, audit_id)
    
    # If there's no linked main audit (i.e., value is 0), default to the current audit ID
    if linked_main_audit_id == 0:
        linked_main_audit_id = audit_id
    print("Linked Main Audit ID:", linked_main_audit_id)

    main_form_id = fetch_from_audits_form_id(connection, linked_main_audit_id)
    print("Main Form Id:- ", main_form_id)

    authorization, email_next = users_func(connection, bot_user_id)
    print("authorization:- ", authorization, "email_next:- ", email_next)

    response = form_status_update(url, main_form_id, 1, bot_user_id, authorization)
    print("Response:- ", response)


    return "DesignX"


default_args={
    'owner': 'Aviral_Tanwar',
    'retries': 0,
    # 'start_date' : datetime.datetime(2024, 7, 7, 17, 11),
}

with DAG(
    dag_id='CompanyName_ProjectName_GlobalStandardBot_form_status_change',                      # This should be unique across all dags on that VM. Make sure to follow the dag nomenclature mentioned in the ppt.
    default_args=default_args,
    # schedule_interval='* * * * *',                            # If the code needs to run on a schedule/cron, it is defined here. In this case, it means run every minute
    schedule = None,                                            # If there is no scheduling
    catchup=False,                                              # It ensures that the DAG does not backfill missed runs between the start_date and the current date. Only the latest scheduled run will be executed.
    max_active_runs=1,                                          # Ensures only one active run at a time
    max_active_tasks=1,                                              # Only one task instance can run concurrently
    tags = ["Productized_Code", "Excel_Upload", "form_submission_bot", "Aviral_Tanwar"],
    default_view='graph',) as dag:                        
        
        data_fetch = PythonOperator(
            dag=dag,
            task_id="task_1",
            python_callable=main_code
        )

data_fetch