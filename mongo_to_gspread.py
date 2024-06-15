from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from pymongo import MongoClient
import gspread
from google.oauth2.service_account import Credentials
import os
import json

def mongo_etl(**kwargs):
    # Create a MongoDB client instance (provide connection string)
    client = MongoClient("mongodb+srv://")
    db = client['database']  # Select the database (provide database name)
    collection = db['collection']  # Select the collection (provide collection name)

    # Extract and transform data from MongoDB
    pipeline = [
        {
            "$project": {
                "_id": 1,
                "createdAt": 1,
                "updatedAt": 1,
                "email": 1,
            }
        },
        {
            "$match": {
                "_id": {"$not": {"$regex": "12345"}}
            }
        }
    ]  
    # Execute the aggregation pipeline and convert the result to a list
    data = list(collection.aggregate(pipeline))

    # Define the schema for the pandas dataframe
    schema = {
        '_id': str,
        'createdAt': 'datetime64[ns]',
        'updatedAt': 'datetime64[ns]',
        'email': str,
    }

    # Create a pandas dataframe with the specified schema
    df = pd.DataFrame(data, columns=schema.keys())

    # Print the first 10 rows of the dataframe to check the data
    print("------------------------- show data here -------------------------")
    print(df.head(10))

    # Convert the dataframe to JSON and push to XCom
    df_json = df.to_json(orient='split')
    kwargs['ti'].xcom_push(key='mongo_df', value=df_json)

        
    # Convert the dataframe to JSON and push to XCom
    df_json = df.to_json(orient='split')
    kwargs['ti'].xcom_push(key='mongo_df', value=df_json)  # Annotated: Push dataframe to XCom as JSON

def dataframe_to_spreadsheet_task(**kwargs):
    # Pull the dataframe from XCom
    ti = kwargs['ti']
    df_json = ti.xcom_pull(key='mongo_df', task_ids='mongo_etl')  # Pull dataframe from XCom
    df = pd.read_json(df_json, orient='split')  # Convert JSON to pandas dataframe

    # GCP API JSON file Path
    keyfile_path = os.getenv('your_file_path')  # Get the GCP key file path from environment variable

    # Handle out-of-range float values
    df = df.replace([float('inf'), float('-inf')], float('nan')).fillna(0)  # Replace inf values with nan and then fill nan with 0

    # Define the scopes for the Google API
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]  
    credentials = Credentials.from_service_account_file(keyfile_path, scopes=scopes)  # Create credentials using the service account file

    # Authorize the credentials
    gc = gspread.authorize(credentials)  

    # Open the spreadsheet by ID
    sheet_id = ''  # Replace with your actual sheet ID
    sheet = gc.open_by_key(sheet_id)  # Open the Google Sheet using the sheet ID
    worksheet = sheet.get_worksheet(0)  # Get the first worksheet

    # Convert dataframe to list of lists
    data_to_update = [df.columns.values.tolist()] + df.values.tolist()  # Prepare data for updating the worksheet

    # Update the worksheet
    worksheet.update(data_to_update)  # Update the worksheet with the dataframe data

with DAG(
    dag_id="spread_sheet_etl",
    schedule_interval="@hourly",
    start_date=datetime(yyyy, mm, dd, hh),
    tags=["etl"],
    default_args={
    "owner": " ",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    },
    catchup=False,
) as dag:

    mongo_etl = PythonOperator(
        task_id='mongo_etl',
        python_callable=mongo_etl,
        provide_context=True,  # Annotated: Enable context passing for XCom
    )

    dataframe_to_spreadsheet_task = PythonOperator(
        task_id='dataframe_to_spreadsheet',
        python_callable=dataframe_to_spreadsheet_task,
        provide_context=True,  # Annotated: Enable context passing for XCom
    )

    mongo_etl >> dataframe_to_spreadsheet_task