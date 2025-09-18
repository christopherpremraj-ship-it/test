from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.secret_manager import SecretsManagerHook
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowTemplatedJobStartOperator,
)

from airflow.providers.google.cloud.operators.bigquery import (BigQueryInsertJobOperator, BigQueryDeleteTableOperator,
                                                               BigQueryCreateEmptyTableOperator)
from airflow.providers.google.cloud.hooks.dataflow import (
    DataflowHook,
    process_line_and_extract_dataflow_job_id_callback,
)
from airflow.operators.email_operator import EmailOperator


PROJECT_ID = 'vz-it-np-voev-dev-voevdo-0'
REGION = 'us-east4'
GCP_CONNECTION_ID = "sa-vz-it-voev-voevdo-0-app"
LOCATION = "us-east4"
to_email_id = 'christopher.premraj@verizon.com'


default_args = {
    'start_date': datetime(2025, 9, 15),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'email': ['christopher.premraj@verizon.com'],
    'email_on_failure': True
    
}

dag = DAG('dg_voev_voevdo_Vast_Data', description='vastdata',
          start_date=datetime(2025, 9, 15),
		  default_args=default_args,
          catchup=False,
          schedule_interval='14 15 * * *',
          max_active_runs=1)

def print_hello():
    return 'inside print_Hello'

def fetch_secret(**kwargs):
# Create a SecretsManagerHook
    hook = SecretsManagerHook(gcp_conn_id='sa-vz-it-voev-voevdo-0-app')
    x="vastdata"
    secret_value = hook.get_secret(secret_id=x,secret_version='latest')
   
    # Log the fetched secret
#     print("Fetched secret value:", secret_value)
    return secret_value

def get_data_from_bq(**kwargs):
    hook = BigQueryHook(gcp_conn_id=GCP_CONNECTION_ID, delegate_to=None, use_legacy_sql=False)
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f'create or replace table log_dataset.Vast_Data_backup as SELECT * FROM vz-it-np-voev-dev-voevdo-0.log_dataset.Vast_Data ')
    
    return True


data_backup = PythonOperator(
    task_id='get_data_backup',
    python_callable=get_data_from_bq,
    email_on_failure=True,
    dag=dag
)

def truncate_table(**kwargs):
    hook = BigQueryHook(gcp_conn_id=GCP_CONNECTION_ID, delegate_to=None, use_legacy_sql=False)
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f'delete   FROM vz-it-np-voev-dev-voevdo-0.log_dataset.Vast_Data where true')
    
    return True


truncate_table = PythonOperator(
    task_id='truncate_main_table',
    python_callable=truncate_table,
    email_on_failure=True,
    dag=dag
)

def start_dflow():
    dflow_hook = DataflowHook(gcp_conn_id='sa-vz-it-voev-voevdo-0-app')
    url = 'jdbc:sqlserver://TDCWPVASVD009:1433'
    dflow_hook.start_template_dataflow(
    
        job_name="vastdata",
        # project_id='vz-it-pr-voev-voevdo-0',
        dataflow_template='gs://dataflow-templates-us-east4/latest/Jdbc_to_BigQuery',
        parameters={
            "driverJars": "gs://voev-dev-voevdo-0-usmr-warehouse/dataexternal/libs/sqljdbc4.jar",
            "driverClassName": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "connectionURL": url,

            "query": "SELECT * FROM APM.vwApplicationHardwareDetails",

            "bigQueryLoadingTemporaryDirectory": "gs://voev-dev-voevdo-0-usmr-warehouse/dataexternal/server_logs/Vast_Data",

            "outputTable": "vz-it-np-voev-dev-voevdo-0:log_dataset.Vast_Data",

            "username": username,
            "password": password
        },
        variables={

            'ipConfiguration': 'WORKER_IP_PRIVATE',
            'serviceAccountEmail': 'sa-dev-voev-app-voevdo-0@vz-it-np-voev-dev-voevdo-0.iam.gserviceaccount.com',
            'experiments': "disable_runner_v2_reason=java_job_google_template",
            'tempLocation': 'gs://voev-dev-voevdo-0-usmr-warehouse/dataexternal/server_logs/Vast_Data',
            'network': 'shared-np-east',
            'numWorkers': 4,
            'maxWorkers': 8,
            'machineType': 'n2-standard-4',
            'kmsKeyName': 'projects/vz-it-np-d0sv-vsadkms-0/locations/us-east4/keyRings/vz-it-np-kr-corp/cryptoKeys/vz-it-np-kms-voev',
            'runner': 'DataflowRunner',
            'subnetwork': 'https://www.googleapis.com/compute/v1/projects/vz-it-np-exhv-sharedvpc-228116/regions/us-east4/subnetworks/shared-np-east-green-subnet-2'
        },
        location='us-east4'
    )



hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

# fetch_secret_task = PythonOperator(
#         task_id='fetch_secret_task',
#         python_callable=fetch_secret,
#         provide_context=True,
#     )

startdataflow = PythonOperator(
    task_id='start_dataflow',
    python_callable=start_dflow,
    dag=dag
)

email_success = EmailOperator(
        task_id='email_success',
        to=to_email_id,
        subject=f'Prod Airflow Alert :  VastData Load Success',
        html_content=f" <h3>VastData offset table load Success</h3> ",
        trigger_rule='one_success',
        dag=dag
)

final_operator = PythonOperator(task_id='bye_task', python_callable=print_hello, dag=dag)



hello_operator >>data_backup>>truncate_table>>startdataflow >>email_success>> final_operator