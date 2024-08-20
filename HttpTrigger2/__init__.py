import logging
import azure.functions as func
import pandas as pd
from azure.storage.blob import BlobClient
from io import StringIO
import os

# Variable globale pour stocker les IDs utilisateurs
user_ids = []

# Charger un fichier CSV depuis Azure Blob Storage
def load_csv_file(connection_string, container_name, file_name):
    blob_client = BlobClient.from_connection_string(connection_string, container_name, file_name)
    download_stream = blob_client.download_blob()
    csv_content = download_stream.content_as_text()
    df = pd.read_csv(StringIO(csv_content))
    return df

# Initialiser les IDs utilisateurs lors du chargement du module
connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
container_name = "data"
load_csv_file(connection_string, container_name, "clicks_df.csv")

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = "Naaaame"
    user_ids_string = ",".join([str(id) for id in user_ids])

    if user_ids_string:
        return func.HttpResponse(user_ids_string, status_code=200)
    else:
        return func.HttpResponse(
             "Please pass a name on the query string or in the request body",
             status_code=400
        )
