import logging
import azure.functions as func
import pandas as pd
from azure.storage.blob import BlobClient
from io import StringIO, BytesIO
import os
import tempfile
from surprise import dump
from sklearn.metrics.pairwise import cosine_similarity
import json

# Charger les IDs utilisateurs une seule fois lors de l'initialisation du module
def load_user_ids(connection_string, container_name, file_name):
    blob_client = BlobClient.from_connection_string(connection_string, container_name, file_name)
    download_stream = blob_client.download_blob()
    csv_content = download_stream.content_as_text()
    df = pd.read_csv(StringIO(csv_content))
    return df['user_id'].tolist()

# Charger un fichier pickle depuis Azure Blob Storage
def load_pickle_file(connection_string, container_name, file_name):
    blob_client = BlobClient.from_connection_string(connection_string, container_name, file_name)
    download_stream = blob_client.download_blob()
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(download_stream.readall())
        temp_file_path = temp_file.name
    _, model = dump.load(temp_file_path)
    return model

# Charger un fichier CSV depuis Azure Blob Storage
def load_csv_file(connection_string, container_name, file_name):
    blob_client = BlobClient.from_connection_string(connection_string, container_name, file_name)
    download_stream = blob_client.download_blob()
    csv_content = download_stream.content_as_text()
    df = pd.read_csv(StringIO(csv_content))
    return df

# Charger un fichier pickle en DataFrame depuis Azure Blob Storage
def load_pickle_df(connection_string, container_name, file_name):
    blob_client = BlobClient.from_connection_string(connection_string, container_name, file_name)
    download_stream = blob_client.download_blob()
    pickle_content = download_stream.readall()
    df = pd.read_pickle(BytesIO(pickle_content))
    return df

# Initialisation des fichiers et modèles
connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
container_name = "data"
user_ids = load_user_ids(connection_string, container_name, "user_id.csv")
model = load_pickle_file(connection_string, container_name, "model_nmf.pickle")
articles_emb = load_pickle_df(connection_string, container_name, "articles_embeddings.pickle")
clicks_df = load_csv_file(connection_string, container_name, "clicks_df.csv")

def recommend_articles_adj(user_id, clicks_df, articles_emb, model, n=5):
    # Obtenir la liste de tous les IDs d'articles
    all_article_ids = set(clicks_df['click_article_id'].unique())
    
    # Obtenir la liste des IDs d'articles que l'utilisateur a déjà vus
    seen_article_ids = set(clicks_df[clicks_df['user_id'] == user_id]['click_article_id'].unique())
    
    # Obtenir la liste des IDs d'articles que l'utilisateur n'a pas encore vus
    unseen_article_ids = all_article_ids - seen_article_ids
    
    # Obtenir les embeddings des articles vus par l'utilisateur
    seen_articles_emb = articles_emb.loc[list(seen_article_ids)]
    
    # Prédire le score pour chaque article non vu
    predictions = []
    for article_id in unseen_article_ids:
        if article_id in articles_emb.index:
            # Obtenir l'embedding de l'article non vu
            article_emb = articles_emb.loc[article_id].values.reshape(1, -1)
            
            # Calculer la similarité cosinus entre cet article et les articles vus
            similarities = cosine_similarity(article_emb, seen_articles_emb.values).flatten()
            
            # Utiliser la similarité maximale comme ajustement du score
            max_similarity = similarities.max()
            
            # Prédire le score avec le modèle
            pred = model.predict(user_id, article_id)
            
            # Ajuster le score avec la similarité
            adjusted_score = pred.est * max_similarity
            
            predictions.append((article_id, adjusted_score))
    
    # Trier les prédictions par score décroissant
    predictions.sort(key=lambda x: x[1], reverse=True)
    
    # Retourner les n premiers articles
    recommended_articles = [pred[0] for pred in predictions[:n]]
    
    return recommended_articles

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        user_id = req.params.get('user_id')
        if not user_id:
            try:
                req_body = req.get_json()
            except ValueError:
                pass
            else:
                user_id = req_body.get('user_id')

        if user_id:
            logging.info(f'User ID: {user_id}')
            recommendations = recommend_articles_adj(int(user_id), clicks_df, articles_emb, model)
            logging.info(f'Recommendations for user {user_id}: {recommendations}')
            
            # Convertir les recommandations en entiers Python
            recommendations = [int(rec) for rec in recommendations]
            
            return func.HttpResponse(json.dumps(recommendations), mimetype="application/json")
        else:
            return func.HttpResponse(
                "Veuillez fournir un identifiant utilisateur.",
                status_code=400
            )
    except Exception as e:
        logging.error(f"Erreur lors du chargement des fichiers ou du modèle: {e}")
        return func.HttpResponse(f"Erreur lors du chargement des fichiers ou du modèle: {e}", status_code=500)


