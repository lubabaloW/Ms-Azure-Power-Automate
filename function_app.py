import azure.functions as func
import logging
import json
import os
from azure.data.tables import TableClient, TableServiceClient
from azure.core.exceptions import ResourceExistsError

app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="cyber/{name}.json",
                               connection="AzureWebJobsStorage") 
def func_blobtrigger(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
    
    try:
        # Read the JSON content from the blob
        blob_content = myblob.read().decode('utf-8')
        json_data = json.loads(blob_content)

        # Extract information from the JSON data
        extracted_data = {
            'PartitionKey': 'project2data',
            'RowKey': os.path.basename(myblob.name),  # Simplify RowKey to just the file name
            'FirstName': json_data.get('firstname', 'DefaultFirstName'),
            'LastName': json_data.get('lastname', 'DefaultLastname'),
            'Gender': json_data.get('gender', 'DefaultGender'),
            'Age': json_data.get('age',0),
            'Value': json_data.get('value', 'DefaultValue')
        }

        # Log the extracted data for debugging
        logging.info(f'Extracted data: {extracted_data}')

        # Connect to Azure Table Storage using environment variable
        conn_str = os.getenv("AzureWebJobsStorage")
        
        if not conn_str:
            logging.error("Azure Table Storage connection string is not set.")
            return
        
        table_service_client = TableServiceClient.from_connection_string(conn_str)
        table_client = table_service_client.get_table_client(table_name="demo")

        # Upsert the entity to handle conflicts
        table_client.upsert_entity(entity=extracted_data)

        logging.info('Data upserted successfully in Table Storage.')

    except Exception as e:
        logging.error(f"Error processing blob: {str(e)}")
        logging.exception("Full traceback:", exc_info=True)

