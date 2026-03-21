# AniData Lab — DAG Anomaly Detector
# Détection d'anomalies : spam, mono-raters, review bombing

import sys
sys.path.insert(0, '/opt/airflow/scripts')

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from anomaly import (
    detect_spam_users, detect_mono_raters,
    detect_suspicious_ratings, anomaly_report,
)


default_args = {
    "owner": "anidata-lab",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


def _spam(**context):
    result = detect_spam_users(min_ratings=5000)
    print(f"Spam users détectés : {result['spam_users']} (seuil: {result['threshold']})")
    print(f"User avec le plus de ratings : {result['max_ratings']}")


def _mono(**context):
    result = detect_mono_raters()
    print(f"Mono-raters détectés : {result['mono_raters']}")


def _bombing(**context):
    result = detect_suspicious_ratings()
    if result.get("status") == "skipped":
        print(f"Skipped : {result['reason']}")
        return
    print(f"Animes potentiellement review-bombés : {result['review_bombed']}")
    for anime in result.get("worst_offenders", [])[:5]:
        print(f"  {anime['Name']} (score1_ratio={anime['score1_ratio']:.2%})")


def _report(**context):
    result = anomaly_report()
    print(result["content"])


with DAG(
    dag_id="04_anomaly_detector",
    default_args=default_args,
    description="Détection d'anomalies : spam, mono-raters, review bombing",
    schedule_interval=None,
    start_date=datetime(2026, 3, 23),
    catchup=False,
    tags=["anidata", "anomaly"],
) as dag:

    t_spam = PythonOperator(
        task_id="detect_spam_users",
        python_callable=_spam,
    )

    t_mono = PythonOperator(
        task_id="detect_mono_raters",
        python_callable=_mono,
    )

    t_bombing = PythonOperator(
        task_id="detect_review_bombing",
        python_callable=_bombing,
    )

    t_report = PythonOperator(
        task_id="anomaly_report",
        python_callable=_report,
    )

    [t_spam, t_mono, t_bombing] >> t_report
