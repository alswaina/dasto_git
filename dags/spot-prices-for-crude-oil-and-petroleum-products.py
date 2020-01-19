#-------------------------------------- Chnage Log -------------------------------------------
__author__ = "Anup Kumar"
__copyright__ = "Copyright 2019, KAPSARC EIM Project"
__credits__ = ["Anup Kumar"]
__license__ = "Prop"
__version__ = "1.0.0"
__maintainer__ = "Anup Kumar"
__email__ = "anup.kumar@kapsarc.org"
__status__ = "Production"
#------------------------------------ Change Details ------------------------------------------
# 02/08/2017 - Anup Kumar - Initial commit
# 08/26/2019 - Anup Kumar - Migration to airflow
# 09/10/2019 - Anup Kumar - Updated the FTP to AWS
#----------------------------------------------------------------------------------------------

import os, shutil, requests, csv, urllib, time, datetime, json
import pandas as pd
from requests.auth import HTTPBasicAuth
import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
#from airflow.hooks.postgres_hook import PostgresHook
#from airflow.hooks.S3_hook import S3Hsook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator

DIR_PATH = os.path.abspath(os.path.dirname(__file__))
FILE_ID = 'D5E48F3A-962D-E711-80CF-0050569F708D'
KDS_ID = 'spot-prices-for-crude-oil-and-petroleum-products'
DOWNLOAD_FILE_NAMES = {'RBRTEd.xls': 'brent - RBRTEd.xls', 'RWTCd.xls': 'wti - RBRTEd.xls'}
EIA_URL = 'https://www.eia.gov/dnav/pet/hist_xls/'

def extract_data(DIR_PATH, DOWNLOAD_FILE_NAMES, EIA_URL):
    for k,v in DOWNLOAD_FILE_NAMES.items():
        try:
            r = requests.get(EIA_URL + k, stream=True)
            with open(os.path.join(DIR_PATH, '_Input', v), 'wb') as f:
                shutil.copyfileobj(r.raw, f)
                print('file ({}) is extracted'.format(k))
        except ValueError:
            raise ValueError("Error: While downloading the file ", k)
    print('trask done')
def process_data(DIR_PATH, DOWNLOAD_FILE_NAMES, FILE_ID):
    dict_of_df = {}
    for index, (k,v) in enumerate(DOWNLOAD_FILE_NAMES.items()):
        # Check for the file and process
        if os.path.exists(os.path.join(DIR_PATH, '_Input', v)):
             dict_of_df["df_{}".format(index)] = pd.read_excel(os.path.join(DIR_PATH, '_Input', v), sheet_name="Data 1", skiprows=2)
        else:
            raise ValueError("Error: File not found ", v)
        
    merged_df = pd.merge(dict_of_df['df_0'], dict_of_df['df_1'], on="Date", how='outer')
    merged_df.to_csv(os.path.join(DIR_PATH, '_Output', FILE_ID + '.csv'),mode = 'w', index=False)
    return

def clear_folders(DIR_PATH, FOLDER_NAME, FILE_NAME):
    try:
        os.remove(os.path.join(DIR_PATH, FOLDER_NAME, FILE_NAME))
        return "Success"
    except ValueError:
        raise ValueError("Unable to remove the input file")

def clear_input_folder(DIR_PATH, FOLDER_NAME, DOWNLOAD_FILE_NAMES):
    for k,v in DOWNLOAD_FILE_NAMES.items():
        try:
            os.remove(os.path.join(DIR_PATH, FOLDER_NAME, v))
        except ValueError:
            raise ValueError("Unable to remove the input file")
    return "Success"

def push_to_aws(FILE_ID):
    s3_hook = S3Hook(aws_conn_id='s3_conn')
    s3_hook.get_conn()
    local_path = os.path.join(DIR_PATH, '_Output', FILE_ID+'.csv')
    print('calling .. Variable.get()')
    #s3_hook.load_file(filename=local_path, key=FILE_ID+'.csv', bucket_name=Variable.get("s3_datasets_bucket_name"), replace=True)
    return "Sucess: Pushed the output"

def get_dataset_uid(dataset_id):
    try:
        print('calling Variable.get()')
        #res = requests.get(Variable.get("kds_api_url") + "catalog/datasets/" + dataset_id, auth=HTTPBasicAuth(Variable.get("kds_auth_email").strip(), Variable.get("kds_auth_password").strip()))
        if res.status_code == 200:
            return res.json()['dataset']['dataset_uid']
    except ValueError:
        raise ValueError("Error: While getting the dataset uid")

def update_dataset_metadata(uid, template, metadata_name, attribute, value):
    payload = '{"value" : "'+value+'" , "override_remote_value": true}'
    try:
        res = requests.put(Variable.get("kds_api_management_url") + '/' + uid + '/' + template + '/' + metadata_name + '/' + attribute, data=payload, auth=HTTPBasicAuth(Variable.get("kds_auth_email").strip(), Variable.get("kds_auth_password").strip()))
        if res.status_code == 200:
            return res
    except ValueError:
        raise ValueError("Error while updating metadata on KDS")

def publish(dataset_id):
    dataset_uid = get_dataset_uid(dataset_id)
    update_metadata = update_dataset_metadata(dataset_uid, 'metadata', 'custom', 'last-checked-date', datetime.datetime.now().strftime("%Y-%m-%d"))
    if update_metadata.status_code == 200:
        try:
            print('calling  Variable.get()')
            #response = requests.put(Variable.get("kds_api_management_url") + '/' + dataset_uid + '/publish/', auth=HTTPBasicAuth(Variable.get("kds_auth_email").strip(), Variable.get("kds_auth_password").strip()))
            return response.json()
        except ValueError:
            raise ValueError("Error while publishing the dataset_uid")

def print_hello():
    return 'pass check'

default_args = {
    'owner': 'Anup Kumar',
    'depends_on_past': False,
    #'start_date': airflow.utils.dates.days_ago(1),
    'start_date': datetime.datetime(2020, 1, 15, 9, 44),
    #'email': [Variable.get("alert_email_to")],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=10)
}

dag = DAG(
    'spot-prices-for-crude-oil-and-petroleum-products',
    default_args=default_args,
    description='World Daily Spot Prices for Crude Oil WTI and Brent',
    schedule_interval='*/5 * * * *',
    catchup=True
)

start_task = DummyOperator(task_id="Start", dag=dag)
end_task = DummyOperator(task_id="End", dag=dag)
t1 = BashOperator(
    task_id='task1',
    bash_command='echo t1', 
    dag=dag) 

t2 = BashOperator(
    task_id='task2',
    bash_command='echo t2', 
    dag=dag)

data_extraction = PythonOperator(
                    task_id = 'data_extraction',
                    #python_callable=extract_data,
                    python_callable=print_hello,
                    #op_kwargs={'DIR_PATH': DIR_PATH, 'DOWNLOAD_FILE_NAMES': DOWNLOAD_FILE_NAMES, 'EIA_URL': EIA_URL},
                    #start_date=airflow.utils.dates.days_ago(0),
                    dag=dag
                )
'''
processing_data = PythonOperator(
                    task_id = 'processing_data',
                    python_callable=process_data,
                    op_kwargs={'DIR_PATH': DIR_PATH, 'DOWNLOAD_FILE_NAMES': DOWNLOAD_FILE_NAMES, 'FILE_ID': FILE_ID},
                    start_date=airflow.utils.dates.days_ago(0),
                    dag=dag
                )

load_dataset_to_aws = PythonOperator(
                    task_id="push_to_aws", 
                    python_callable=push_to_aws,
                    op_kwargs={'FILE_ID': FILE_ID},
                    dag=dag
                )

publish_dataset = PythonOperator(
                task_id="PublishDataset", 
                python_callable=publish,
                op_kwargs={'dataset_id': KDS_ID},
                dag=dag
            )

clear_input = PythonOperator(
                task_id="ClearInput", 
                python_callable=clear_input_folder,
                op_kwargs={'DIR_PATH': DIR_PATH, 'FOLDER_NAME': '_Input', 'DOWNLOAD_FILE_NAMES': DOWNLOAD_FILE_NAMES},
                dag=dag
            )

clear_output = PythonOperator(
                task_id="ClearOutput", 
                python_callable=clear_folders,
                op_kwargs={'DIR_PATH': DIR_PATH, 'FOLDER_NAME': '_Output', 'FILE_NAME': FILE_ID+'.csv'},
                dag=dag
            )
'''

#start_task >> data_extraction >> processing_data >> load_dataset_to_aws >> publish_dataset >> clear_input >> clear_output >> end_task

start_task >> t1 >> data_extraction >> t2 >> end_task