# AniData Lab — DAG Convert
# Conversion XML/JSON → CSV avant la phase Extract
# Dossier source : /opt/airflow/data/imports/
# Dossier sortie : /opt/airflow/data/imports/  (les CSV convertis restent dans imports)

import sys
sys.path.insert(0, '/opt/airflow/scripts')

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from convert import scan_rep


default_args = {
    "owner": "anidata-lab",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="00b_convert_to_csv",
    default_args=default_args,
    description="Conversion XML/JSON → CSV (imports/ → converted/)",
    schedule_interval=None,
    start_date=datetime(2026, 3, 23),
    catchup=False,
    tags=["anidata", "convert"],
) as dag:

    convert = PythonOperator(
        task_id="convert_files",
        python_callable=scan_rep,
    )
