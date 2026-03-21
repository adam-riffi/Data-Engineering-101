# AniData Lab — DAG Transform
# Nettoyage, feature engineering, export gold avec versioning

import sys
sys.path.insert(0, '/opt/airflow/scripts')

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

from transform import (
    clean_anime, clean_synopsis, merge_datasets,
    feature_engineering, check_gold_exists, export_gold,
)


default_args = {
    "owner": "anidata-lab",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def _clean_anime(**context):
    result = clean_anime()
    print(f"Anime nettoyé : {result['rows']} lignes, {result['nulls']} NaN total")


def _clean_synopsis(**context):
    result = clean_synopsis()
    print(f"Synopsis nettoyé : {result['rows']} lignes")


def _merge(**context):
    result = merge_datasets()
    print(f"Merge : {result['rows']} lignes, {result['with_synopsis']} avec synopsis")


def _features(**context):
    result = feature_engineering()
    print(f"Features ajoutées : {result['new_features']}")


def _check_version(**context):
    return check_gold_exists()


def _export_first(**context):
    result = export_gold(versioned=False)
    print(f"Premier export gold v{result['version']} : {result['rows']} lignes")
    context["ti"].xcom_push(key="gold_path", value=result["csv"])


def _export_versioned(**context):
    result = export_gold(versioned=True)
    print(f"Export gold v{result['version']} : {result['rows']} lignes")
    context["ti"].xcom_push(key="gold_path", value=result["csv"])


with DAG(
    dag_id="02_transform_anime",
    default_args=default_args,
    description="Transform : nettoyage + features + export gold",
    schedule_interval=None,
    start_date=datetime(2026, 3, 23),
    catchup=False,
    tags=["anidata", "transform"],
) as dag:

    t_clean_anime = PythonOperator(
        task_id="clean_anime",
        python_callable=_clean_anime,
    )

    t_clean_synopsis = PythonOperator(
        task_id="clean_synopsis",
        python_callable=_clean_synopsis,
    )

    t_merge = PythonOperator(
        task_id="merge_datasets",
        python_callable=_merge,
    )

    t_features = PythonOperator(
        task_id="feature_engineering",
        python_callable=_features,
    )

    t_check = BranchPythonOperator(
        task_id="check_gold_version",
        python_callable=_check_version,
    )

    t_export_first = PythonOperator(
        task_id="export_gold_first",
        python_callable=_export_first,
    )

    t_export_versioned = PythonOperator(
        task_id="export_gold_versioned",
        python_callable=_export_versioned,
    )

    [t_clean_anime, t_clean_synopsis] >> t_merge >> t_features >> t_check
    t_check >> t_export_first
    t_check >> t_export_versioned
