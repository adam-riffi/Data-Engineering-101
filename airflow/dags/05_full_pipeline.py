# AniData Lab — Pipeline ETL Complet
# Chaîne Extract > Transform > Load puis déclenche la détection d'anomalies
# C'est le DAG "one click" demandé dans le livrable final

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "anidata-lab",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def _log_start(**context):
    print("Pipeline ETL AniData Lab — Démarrage")
    print("Extract > Transform > Load > Anomaly Detection")


def _log_end(**context):
    print("Pipeline ETL terminé. Vérifiez Grafana : http://localhost:3000")


with DAG(
    dag_id="05_full_pipeline",
    default_args=default_args,
    description="Pipeline complet E>T>L + anomalies, déclenchable en un clic",
    schedule_interval=None,
    start_date=datetime(2026, 3, 23),
    catchup=False,
    tags=["anidata", "pipeline"],
) as dag:

    start = PythonOperator(
        task_id="log_start",
        python_callable=_log_start,
    )

    trigger_convert = TriggerDagRunOperator(
        task_id="trigger_convert",
        trigger_dag_id="00b_convert_to_csv",
        wait_for_completion=True,
        poke_interval=10,
    )

    trigger_extract = TriggerDagRunOperator(
        task_id="trigger_extract",
        trigger_dag_id="01_extract_anime",
        wait_for_completion=True,
        poke_interval=10,
    )

    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_transform",
        trigger_dag_id="02_transform_anime",
        wait_for_completion=True,
        poke_interval=10,
    )

    trigger_load = TriggerDagRunOperator(
        task_id="trigger_load",
        trigger_dag_id="03_load_anime",
        wait_for_completion=True,
        poke_interval=10,
    )

    trigger_anomaly = TriggerDagRunOperator(
        task_id="trigger_anomaly",
        trigger_dag_id="04_anomaly_detector",
        wait_for_completion=True,
        poke_interval=10,
    )

    end = PythonOperator(
        task_id="log_end",
        python_callable=_log_end,
    )

    start >> trigger_convert >> trigger_extract >> trigger_transform >> trigger_load >> trigger_anomaly >> end
