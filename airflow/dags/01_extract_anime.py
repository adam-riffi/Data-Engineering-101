# AniData Lab — DAG Extract
# Lecture des 3 CSV sources en parallèle + validation de schéma

import sys
sys.path.insert(0, '/opt/airflow/scripts')

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from extract import extract_csv, validate_schema


default_args = {
    "owner": "anidata-lab",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def _extract_anime(**context):
    result = extract_csv("anime")
    context["ti"].xcom_push(key="extract_anime", value=result)
    print(f"anime.csv : {result['rows']} lignes")


def _extract_ratings(**context):
    result = extract_csv("ratings")
    context["ti"].xcom_push(key="extract_ratings", value=result)
    print(f"rating_complete.csv : {result['rows']} lignes")


def _extract_synopsis(**context):
    result = extract_csv("synopsis")
    context["ti"].xcom_push(key="extract_synopsis", value=result)
    print(f"anime_with_synopsis.csv : {result['rows']} lignes")


def _validate_all(**context):
    ti = context["ti"]
    datasets = ["anime", "ratings", "synopsis"]

    for ds in datasets:
        extract_result = ti.xcom_pull(key=f"extract_{ds}", task_ids=f"extract_{ds}")
        validation = validate_schema(ds, extract_result)
        print(f"{ds} : {validation['status']} ({validation['rows']} lignes, {validation['columns_checked']} colonnes vérifiées)")

    print("Extraction et validation terminées")


with DAG(
    dag_id="01_extract_anime",
    default_args=default_args,
    description="Extract : lecture des CSV sources + validation schéma",
    schedule_interval=None,
    start_date=datetime(2026, 3, 23),
    catchup=False,
    tags=["anidata", "extract"],
) as dag:

    extract_anime = PythonOperator(
        task_id="extract_anime",
        python_callable=_extract_anime,
    )

    extract_ratings = PythonOperator(
        task_id="extract_ratings",
        python_callable=_extract_ratings,
    )

    extract_synopsis = PythonOperator(
        task_id="extract_synopsis",
        python_callable=_extract_synopsis,
    )

    validate = PythonOperator(
        task_id="validate_schema",
        python_callable=_validate_all,
    )

    [extract_anime, extract_ratings, extract_synopsis] >> validate
