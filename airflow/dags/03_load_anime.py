# AniData Lab — DAG Load
# Indexation bulk du dataset gold dans Elasticsearch

import sys
sys.path.insert(0, '/opt/airflow/scripts')

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from load import create_index_mapping, bulk_index, verify_index


default_args = {
    "owner": "anidata-lab",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def _create_mapping(**context):
    result = create_index_mapping()
    print(f"Index '{result['index']}' : {result['status']}")


def _bulk_index(**context):
    result = bulk_index()
    print(f"Indexation : {result['indexed']}/{result['total']} docs, {result['errors']} erreurs")


def _verify(**context):
    result = verify_index()
    print(f"Vérification : {result['doc_count']} docs dans '{result['index']}'")
    print(f"Exemples : {result['sample_titles']}")


with DAG(
    dag_id="03_load_anime",
    default_args=default_args,
    description="Load : indexation bulk dans Elasticsearch + vérification",
    schedule_interval=None,
    start_date=datetime(2026, 3, 23),
    catchup=False,
    tags=["anidata", "load"],
) as dag:

    t_mapping = PythonOperator(
        task_id="create_index_mapping",
        python_callable=_create_mapping,
    )

    t_index = PythonOperator(
        task_id="bulk_index",
        python_callable=_bulk_index,
    )

    t_verify = PythonOperator(
        task_id="verify_index",
        python_callable=_verify,
    )

    t_mapping >> t_index >> t_verify
