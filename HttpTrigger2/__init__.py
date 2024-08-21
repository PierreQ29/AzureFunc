import os
import pickle
import logging
import pandas as pd
import azure.functions as func
from io import StringIO, BytesIO
from azure.storage.blob import BlobClient
from sklearn.metrics.pairwise import cosine_similarity

# Charger les IDs utilisateurs une seule fois lors de l'initialisation du module
def load_user_ids(connection_string, container_name, file_name):
    blob_client = BlobClient.from_connection_string(connection_string, container_name, file_name)
    download_stream = blob_client.download_blob()
    csv_content = download_stream.content_as_text()
    df = pd.read_csv(StringIO(csv_content))
    return df['user_id'].tolist()
   
# Charger le fichier clicks depuis Azure Blob Storage
def load_clicks_file(connection_string, container_name, file_name):
    blob_client = BlobClient.from_connection_string(connection_string, container_name, file_name)
    download_stream = blob_client.download_blob()
    csv_content = download_stream.content_as_text()
    df = pd.read_csv(StringIO(csv_content))
    return df

# Charger le fichier embedding depuis Azure Blob Storage
def load_article_embeddings(connection_string, container_name, file_name):
    blob_client = BlobClient.from_connection_string(connection_string, container_name, file_name)
    download_stream = blob_client.download_blob()
    pickle_content = download_stream.readall()
    articles_emb = pd.read_pickle(BytesIO(pickle_content))
    articles_emb = pd.DataFrame(articles_emb, columns=["embedding_" + str(i) for i in range(articles_emb.shape[1])])
    return articles_emb

# load surprise model by pickle
def load_model(connection_string, container_name, file_name_model):
    blob_client = BlobClient.from_connection_string(connection_string, container_name, file_name_model)
    download_stream = blob_client.download_blob()
   
    # Lire le contenu du modèle en tant que bytes
    model_content = download_stream.readall()
   
    # Charger le modèle directement depuis le contenu en mémoire
    file_like_object = BytesIO(model_content)
   
    # Utiliser dump.load pour charger correctement le modèle et les prédictions
    model = pickle.load(file_like_object)
   
    if isinstance(model, dict) and 'algo' in model:
        return model['algo']
    else:
        raise ValueError("Le modèle chargé ne contient pas l'algorithme attendu.---load_model()----pickle")


# recommend_articles pour int user_id
def recommend_articles_adj(user_id, clicks_df, articles_emb, model, n=5):
    all_article_ids = set(clicks_df['click_article_id'].unique())
    seen_article_ids = set(clicks_df[clicks_df['user_id'] == user_id]['click_article_id'].unique())
   
    if not seen_article_ids:
        raise ValueError(f"L'utilisateur {user_id} n'a consulté aucun article.")
   
    unseen_article_ids = all_article_ids - seen_article_ids
   
    # Vérifiez que les articles vus existent bien dans les embeddings
    valid_seen_article_ids = [aid for aid in seen_article_ids if aid in articles_emb.index]
   
    if not valid_seen_article_ids:
        raise ValueError(f"Aucun des articles vus par l'utilisateur {user_id} n'a d'embeddings valides.")
   
    seen_articles_emb = articles_emb.loc[valid_seen_article_ids]
   
    predictions = []
    for article_id in unseen_article_ids:
        if article_id in articles_emb.index:
            article_emb = articles_emb.loc[article_id].values.reshape(1, -1)
           
            # Vérifiez que les embeddings ne sont pas vides avant de calculer la similarité
            if article_emb.size == 0 or seen_articles_emb.size == 0:
                continue
               
            similarities = cosine_similarity(article_emb, seen_articles_emb.values).flatten()
            max_similarity = similarities.max()
            pred = model.predict(user_id, article_id)
            adjusted_score = pred.est * max_similarity
            predictions.append((int(article_id), float(adjusted_score)))
   
    predictions.sort(key=lambda x: x[1], reverse=True)
    recommended_articles = [pred[0] for pred in predictions[:n]]
   
    return recommended_articles

# ======================== #
connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
container_name = "data"
#
# Charger les IDs utilisateurs lors de l'initialisation
user_ids = load_user_ids(connection_string, container_name, "user_id.csv")
#
# Charger clicks_file
clicks_df = load_clicks_file(connection_string, container_name, "clicks_df.csv")
#
# Charger article_embeddings
articles_emb = load_article_embeddings(connection_string, container_name, "articles_embeddings.pickle")
#
# Charger model
model = load_model(connection_string, container_name, "model_nmf.pickle")


# ========= main ========== #  

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    str_test = ""
    try:
        #
        str_test = ",".join([str(id) for id in user_ids])
        str_test += " === " + str(clicks_df['click_article_id'][2]) + " === " +  str(articles_emb['embedding_0'][2])
        #
        user_id = req.params.get('user_id')
        recommendations = []
        if user_id:
            recommendations = recommend_articles_adj(int(user_id), clicks_df, articles_emb, model)
        #
        article_ids_string = ",".join([str(id) for id in recommendations])
        str_test = str_test + " === " + article_ids_string
        #
    except Exception as ee:
        print("============")
        print(ee)
        print("============")
       
    if user_id :
        return func.HttpResponse(article_ids_string, status_code=200)
    else:
        return func.HttpResponse(
             "Please pass a user_id on the query string or in the request body",
             status_code=400
        )
