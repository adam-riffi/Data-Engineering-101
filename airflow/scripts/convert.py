import os
import pandas as pd


REP_IMPORTS = "/opt/airflow/data/imports"   # raw user files + converted outputs


def json_to_csv(filepath):
    df = pd.read_json(filepath)
    dst = os.path.join(REP_IMPORTS, os.path.basename(filepath).replace(".json", ".csv"))
    df.to_csv(dst, index=False)
    return dst


def xml_to_csv(filepath):
    df = pd.read_xml(filepath)
    dst = os.path.join(REP_IMPORTS, os.path.basename(filepath).replace(".xml", ".csv"))
    df.to_csv(dst, index=False)
    return dst


def scan_rep():
    os.makedirs(REP_IMPORTS, exist_ok=True)
    for file in os.listdir(REP_IMPORTS):
        path = os.path.join(REP_IMPORTS, file)
        if file.endswith(".json"):
            json_to_csv(path)
        elif file.endswith(".xml"):
            xml_to_csv(path)
