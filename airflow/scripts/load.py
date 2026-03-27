import os
from datetime import datetime, timezone
import pandas as pd
from elasticsearch import Elasticsearch, helpers

REP_EXPORTS = "/opt/airflow/data/exports"   # gold CSV produced by transform
ES_HOST = "http://elasticsearch:9200"
INDEX_NAME = "anime"

def get_es_client():
    es = Elasticsearch(ES_HOST)
    if not es.ping():
        raise ConnectionError(f"Elasticsearch injoignable à {ES_HOST}")
    return es

# Supprime l'ancien index et en crée un nouveau avec le bon mapping
def create_index_mapping():
    es = get_es_client()

    mapping = {
        "mappings": {
            "properties": {
                "MAL_ID": {"type": "integer"},
                "Name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "English name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "Japanese name": {"type": "text", "analyzer": "standard"},
                "Score": {"type": "float"},
                "Genres": {"type": "keyword"},
                "Type": {"type": "keyword"},
                "Episodes": {"type": "integer"},
                "Studios": {"type": "keyword"},
                "Source": {"type": "keyword"},
                "Rating": {"type": "keyword"},
                "Ranked": {"type": "float"},
                "Popularity": {"type": "integer"},
                "Members": {"type": "integer"},
                "Favorites": {"type": "integer"},
                "Watching": {"type": "integer"},
                "Completed": {"type": "integer"},
                "On-Hold": {"type": "integer"},
                "Dropped": {"type": "integer"},
                "Plan to Watch": {"type": "integer"},
                "Synopsis": {"type": "text"},
                "weighted_score": {"type": "float"},
                "drop_ratio": {"type": "float"},
                "studio_tier": {"type": "keyword"},
                "duration_min": {"type": "float"},
                "indexed_at": {"type": "date"},
            }
        },
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
        }
    }

    if es.indices.exists(index=INDEX_NAME):
        es.indices.delete(index=INDEX_NAME)
        print(f"Index '{INDEX_NAME}' supprimé")

    es.indices.create(index=INDEX_NAME, mappings=mapping["mappings"], settings=mapping["settings"])
    return {"index": INDEX_NAME, "status": "created"}


# Transforme une ligne pandas en document ES
# Les champs multi-valués (Genres, Studios) sont splittés en listes
def prepare_doc(row, indexed_at):
    doc = {"indexed_at": indexed_at}
    for k, v in row.items():
        if pd.isna(v):
            continue
        if k in ("Genres", "Studios", "Producers", "Licensors"):
            doc[k] = [x.strip() for x in str(v).split(",")]
        else:
            doc[k] = v
    return doc


# Lit le gold CSV et indexe tout dans ES par batch de 500
def bulk_index():
    gold_path = os.path.join(REP_EXPORTS, "anime_gold_latest.csv")
    if not os.path.exists(gold_path):
        raise FileNotFoundError("anime_gold_latest.csv introuvable. Lancez le DAG Transform d'abord.")

    df = pd.read_csv(gold_path)
    es = get_es_client()

    indexed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    actions = []
    for _, row in df.iterrows():
        doc = prepare_doc(row, indexed_at)
        actions.append({
            "_index": INDEX_NAME,
            "_id": int(row["MAL_ID"]),
            "_source": doc,
        })

    success, errors = helpers.bulk(es, actions, raise_on_error=False, chunk_size=500)
    return {
        "indexed": success,
        "errors": len(errors) if isinstance(errors, list) else errors,
        "total": len(df),
    }


# Refresh l'index et vérifie le nombre de docs indexés
def verify_index():
    es = get_es_client()
    es.indices.refresh(index=INDEX_NAME)
    count = es.count(index=INDEX_NAME)["count"]

    sample = es.search(index=INDEX_NAME, query={"match_all": {}}, size=3)
    titles = [hit["_source"].get("Name", "?") for hit in sample["hits"]["hits"]]

    return {
        "index": INDEX_NAME,
        "doc_count": count,
        "sample_titles": titles,
    }
