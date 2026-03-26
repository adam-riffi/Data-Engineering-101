# AniData Lab — DAG Load
# Indexation bulk du dataset gold dans Elasticsearch via Logstash
# Solution compatible Windows : le CSV est injecté directement dans un volume Docker
# pour contourner les problèmes de symlinks VirtioFS sur Windows.

import sys
sys.path.insert(0, '/opt/airflow/scripts')

import io
import os
import socket
import tarfile
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from load import create_index_mapping, verify_index


default_args = {
    "owner": "anidata-lab",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def _create_mapping(**context):
    result = create_index_mapping()
    print(f"Index '{result['index']}' : {result['status']}")


def _run_logstash(**context):
    import docker

    client = docker.DockerClient(base_url='unix://var/run/docker.sock')

    # Découverte dynamique du chemin hôte du pipeline et du réseau
    hostname = socket.gethostname()
    airflow_container = client.containers.get(hostname)

    host_pipeline_path = None
    for mount in airflow_container.attrs['Mounts']:
        if mount['Destination'] == '/opt/airflow/logstash_pipeline':
            host_pipeline_path = mount['Source']

    if not host_pipeline_path:
        raise ValueError("Mount /opt/airflow/logstash_pipeline introuvable sur le conteneur Airflow")

    networks = list(airflow_container.attrs['NetworkSettings']['Networks'].keys())
    network_name = next((n for n in networks if 'anidata' in n), networks[0])

    print(f"Chemin hôte pipeline : {host_pipeline_path}")
    print(f"Réseau Docker        : {network_name}")

    # Localisation du CSV dans le conteneur Airflow courant
    # On résout le symlink si nécessaire (transform.py crée anime_gold_latest.csv → anime_gold_vN.csv)
    gold_path = '/opt/airflow/data/anime_gold_latest.csv'
    if os.path.islink(gold_path):
        gold_path = os.path.realpath(gold_path)
        print(f"Symlink résolu → {gold_path}")
    if not os.path.exists(gold_path):
        # Fallback : chercher le dernier fichier versionné directement
        import glob
        candidates = sorted(glob.glob('/opt/airflow/data/anime_gold_v*.csv'))
        if not candidates:
            raise FileNotFoundError("Aucun fichier anime_gold trouvé. Avez-vous lancé le DAG 02 ?")
        gold_path = candidates[-1]
        print(f"Fallback fichier versionné : {gold_path}")

    file_size = os.path.getsize(gold_path)
    print(f"Fichier CSV : {os.path.basename(gold_path)} ({file_size:,} octets)")

    # Nettoyage des ressources résiduelles
    container_name = "anidata-logstash-run"
    volume_name = "anidata-logstash-data-tmp"

    try:
        client.containers.get(container_name).remove(force=True)
        print(f"Conteneur résiduel supprimé : {container_name}")
    except docker.errors.NotFound:
        pass

    try:
        client.volumes.get(volume_name).remove()
    except docker.errors.NotFound:
        pass

    # Création d'un volume temporaire et injection du CSV via put_archive
    # Cela évite tous les problèmes de symlinks/chemins Windows sur le bind mount data
    client.volumes.create(name=volume_name)

    with open(gold_path, 'rb') as f:
        csv_data = f.read()

    tar_stream = io.BytesIO()
    with tarfile.open(fileobj=tar_stream, mode='w') as tar:
        info = tarfile.TarInfo(name='anime_gold_latest.csv')
        info.size = len(csv_data)
        tar.addfile(info, io.BytesIO(csv_data))
    tar_stream.seek(0)

    # Conteneur helper pour copier le fichier dans le volume.
    # Important : le conteneur doit être DÉMARRÉ pour que le volume soit monté
    # et que put_archive écrive bien dans le volume (pas dans la couche du conteneur).
    helper = client.containers.run(
        image='alpine:latest',
        command=['sleep', '30'],
        volumes={volume_name: {'bind': '/data', 'mode': 'rw'}},
        detach=True,
    )
    tar_stream.seek(0)
    helper.put_archive('/data/', tar_stream)
    helper.stop(timeout=1)
    helper.remove()
    print(f"CSV injecté dans le volume '{volume_name}' ({file_size:,} octets)")

    # Lancement de Logstash : volume pour les données, bind mount pour le pipeline
    container = client.containers.run(
        image="docker.elastic.co/logstash/logstash:8.12.0",
        name=container_name,
        network=network_name,
        volumes={
            volume_name: {"bind": "/usr/share/logstash/data_input", "mode": "ro"},
            host_pipeline_path: {"bind": "/usr/share/logstash/pipeline", "mode": "ro"},
        },
        environment={
            "LS_JAVA_OPTS": "-Xms2g -Xmx2g",
            "xpack.monitoring.enabled": "false",
        },
        detach=True,
    )

    print(f"Conteneur Logstash démarré : {container.id[:12]}")

    # Streaming des logs en temps réel
    for line in container.logs(stream=True, follow=True):
        print(line.decode("utf-8", errors="replace").rstrip())

    # Attente de la fin et vérification du code de sortie
    result = container.wait()
    exit_code = result.get("StatusCode", -1)
    print(f"Logstash terminé avec code : {exit_code}")

    container.remove()
    client.volumes.get(volume_name).remove()

    if exit_code != 0:
        raise Exception(f"Logstash a échoué avec le code de sortie {exit_code}")


def _verify(**context):
    result = verify_index()
    print(f"Vérification : {result['doc_count']} docs dans '{result['index']}'")
    print(f"Exemples : {result['sample_titles']}")


with DAG(
    dag_id="03_load_anime",
    default_args=default_args,
    description="Load : mapping ES + indexation via Logstash (volume Docker, compatible Windows) + vérification",
    schedule_interval=None,
    start_date=datetime(2026, 3, 23),
    catchup=False,
    tags=["anidata", "load", "logstash"],
) as dag:

    t_mapping = PythonOperator(
        task_id="create_index_mapping",
        python_callable=_create_mapping,
    )

    t_logstash = PythonOperator(
        task_id="run_logstash",
        python_callable=_run_logstash,
        execution_timeout=timedelta(minutes=45),
    )

    t_verify = PythonOperator(
        task_id="verify_index",
        python_callable=_verify,
    )

    t_mapping >> t_logstash >> t_verify
