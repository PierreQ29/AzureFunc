import logging
import azure.functions as func
from azure.storage.blob import BlobClient
import os
import tempfile
from surprise import dump

# Charger un fichier pickle depuis Azure Blob Storage
def load_model(connection_string, container_name, file_name):
    try:
        blob_client = BlobClient.from_connection_string(connection_string, container_name, file_name)
        download_stream = blob_client.download_blob()
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(download_stream.readall())
            temp_file_path = temp_file.name
        _, model = dump.load(temp_file_path)
        return model
    except Exception as e:
        logging.error(f"Erreur lors du chargement du modèle: {e}")
        return None

# Initialisation du modèle
connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
container_name = "data"
model = load_model(connection_string, container_name, "model_nmf.pickle")

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    if model is None:
        return func.HttpResponse(
            "Erreur lors du chargement du modèle.",
            status_code=500
        )

    return func.HttpResponse(
        "Le modèle a été chargé avec succès.",
        status_code=200
    )
