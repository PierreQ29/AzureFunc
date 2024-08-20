import logging
import azure.functions as func
import pandas as pd
from azure.storage.blob import BlobClient
from io import StringIO
import os

# Charger les IDs utilisateurs une seule fois lors de l'initialisation du module
def load_user_ids(connection_string, container_name, file_name):
    blob_client = BlobClient.from_connection_string(connection_string, container_name, file_name)
    download_stream = blob_client.download_blob()
    csv_content = download_stream.content_as_text()
    df = pd.read_csv(StringIO(csv_content))
    return df['user_id'].tolist()
    
# Charger un fichier CSV depuis Azure Blob Storage
def load_csv_file(connection_string, container_name, file_name):
    blob_client = BlobClient.from_connection_string(connection_string, container_name, file_name)
    download_stream = blob_client.download_blob()
    csv_content = download_stream.content_as_text()
    df = pd.read_csv(StringIO(csv_content))
    return df

# def load_article_embeddings(connection_string, container_name, file_name):
#     blob_client = BlobClient.from_connection_string(connection_string, container_name, file_name)
#     download_stream = blob_client.download_blob()
#     pickle_content = download_stream.readall()
#     articles_emb = pd.read_pickle(io.BytesIO(pickle_content))
#     articles_emb = pd.DataFrame(articles_emb, columns=["embedding_" + str(i) for i in range(articles_emb.shape[1])])
#     return articles_emb

# Initialisation des fichiers et modèles
connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
container_name = "data"
user_ids = load_user_ids(connection_string, container_name, "user_id.csv")
clicks_df = load_csv_file(connection_string, container_name, "clicks_df.csv")
# articles_emb = load_article_embeddings(connection_string, container_name, "articles_embeddings.pickle")

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    # Vérification du téléchargement du fichier
    logging.info(f"clicks loaded with {len(clicks_df)} rows.")
    logging.info(f"user loaded with {len(user_ids)} rows.")
    # logging.info(f"embed loaded with {len(articles_emb)} rows.")
    
    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}!")
    else:
        return func.HttpResponse(
             "Please pass a name on the query string or in the request body",
             status_code=400
        )
