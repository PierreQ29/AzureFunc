import logging
import azure.functions as func
import pandas as pd
from azure.storage.blob import BlobClient
from io import StringIO
import os

# Variable globale pour stocker les IDs utilisateurs
user_ids = []

# Charger les IDs utilisateurs une seule fois lors de l'initialisation du module
def load_user_ids(connection_string, container_name, file_name):
    global user_ids
    blob_client = BlobClient.from_connection_string(connection_string, container_name, file_name)
    download_stream = blob_client.download_blob()
    csv_content = download_stream.content_as_text()
    df = pd.read_csv(StringIO(csv_content))
    user_ids = df['user_id'].tolist()

# Initialiser les IDs utilisateurs lors du chargement du module
connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
container_name = "data"
load_user_ids(connection_string, container_name, "user_id.csv")

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
