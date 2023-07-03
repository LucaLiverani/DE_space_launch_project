from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task 
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

import requests
import pandas as pd
import json
from datetime import datetime


from azure.storage.blob import BlobServiceClient
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential


# Databricks Job_IDS to be called
job_id ={
  'ETL_staging': 410423428152468,
  'ETL_dm_agency': 212481872795225,
  'ETL_ft_launch': 595803403015360,
  'ETL_dm_rocket': 899151580482340,
  'ETL_dm_pad': 1065155973600023,
  'ETL_dm_mission': 1072696837639820
}


# Function to handle requesting data
def get_results(query_url: str) -> dict or None:

    try:
        # Requesting data
        results = requests.get(query_url)
    except Exception as e:
        raise(e)
    else:
        # Checking status of the query
        status = results.status_code
        print(f'Status code: {status}')

        # Return when the query status isn't 200 OK
        if status != 200:
            return

        # Converting to JSON and returning
        return results.json()


def upload_df_to_storage(connect_str, container_name,  df, name):
    
    try:

        blob_service_client = BlobServiceClient.from_connection_string(connect_str)

        # Create a file in the local data directory to upload and download
        ct = str(datetime.now())
        ct = ct.replace(" ", "_")
        ct = ct.split('.')[0]
        file_name_in_blob_storage = name + "_" + ct + ".parquet"
        upload_file_path = 'df_' + str(name) + '.parquet.gzip'
        df.to_parquet(upload_file_path,
              compression='gzip')

        # Create a blob client using the local file name as the name for the blob
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name_in_blob_storage)

        print("\nUploading to Azure Storage as blob: " + file_name_in_blob_storage)

        with open(file=upload_file_path, mode="rb") as data:
            blob_client.upload_blob(data)

        # Handle Exeption
    except Exception as e:
       raise(e)
    
def updload_data_to_storage_account():
        
        # URL
        launch_base_url = 'https://lldev.thespacedevs.com/2.2.0/launch/'
        
        # Parameters Blob Storage
        container_name = 'container00001'
        keyVaultName = "key00001"
        KVUri = f"https://{keyVaultName}.vault.azure.net"

        credential = DefaultAzureCredential()
        client = SecretClient(vault_url=KVUri, credential=credential)
        connect_str = client.get_secret('connect_str').value

        # Set mode to detailed to include all related objects
        mode = 'mode=detailed'
        # Limit returned results to just n per query
        limit = 'limit=100'
        # Ordering the results by ascending T-0 (NET)
        ordering = 'ordering=-last_updated'
        # Assemble the query URL
        query_url = launch_base_url + '?' + '&'.join((mode, limit, ordering))

        r_json = get_results(query_url)
        latest_launch_df = pd.json_normalize(r_json.get('results'), max_level=0)
        upload_df_to_storage(connect_str, container_name, latest_launch_df, 'latest_launch')



with DAG('process_space_data', start_date=datetime(2023, 6, 17), schedule_interval='@daily', catchup=False, tags=["test"],) as dag:
    
    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='thespacedevs_api',
        endpoint='2.2.0/launch/'
    )
    
    updload_data_to_storage_account = PythonOperator(
        task_id='updload_data_to_storage_account',
        python_callable=updload_data_to_storage_account
    ) 

    ETL_staging = DatabricksRunNowOperator(
        task_id = 'ETL_staging',
        databricks_conn_id = 'databricks_default',
        job_id = job_id.get('ETL_staging')
    )

    ETL_dm_agency = DatabricksRunNowOperator(
        task_id = 'ETL_dm_agency',
        databricks_conn_id = 'databricks_default',
        job_id = job_id.get('ETL_dm_agency')
    )

    ETL_dm_rocket = DatabricksRunNowOperator(
        task_id = 'ETL_dm_rocket',
        databricks_conn_id = 'databricks_default',
        job_id = job_id.get('ETL_dm_rocket')
    )

    ETL_dm_pad = DatabricksRunNowOperator(
        task_id = 'ETL_dm_pad',
        databricks_conn_id = 'databricks_default',
        job_id = job_id.get('ETL_dm_pad')
    )

    ETL_dm_mission = DatabricksRunNowOperator(
        task_id = 'ETL_dm_mission',
        databricks_conn_id = 'databricks_default',
        job_id = job_id.get('ETL_dm_mission')
    )

    ETL_ft_launch = DatabricksRunNowOperator(
        task_id = 'ETL_ft_launch',
        databricks_conn_id = 'databricks_default',
        job_id = job_id.get('ETL_ft_launch')
    )

    is_api_available >> updload_data_to_storage_account >> ETL_staging >> ETL_dm_mission >> ETL_ft_launch
    is_api_available >> updload_data_to_storage_account >> ETL_staging >> ETL_dm_pad >> ETL_ft_launch
    is_api_available >> updload_data_to_storage_account >> ETL_staging >> ETL_dm_rocket >> ETL_ft_launch
    is_api_available >> updload_data_to_storage_account >> ETL_staging >> ETL_dm_agency >> ETL_ft_launch
