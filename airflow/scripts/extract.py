import os
import pandas as pd


REP_SOURCE = "/opt/airflow/data"

EXPECTED_SCHEMAS = {
    "anime": {
        "file": "anime.csv",
        "required_columns": [
            "MAL_ID", "Name", "Score", "Genres", "Type",
            "Episodes", "Studios", "Source", "Members",
            "Favorites", "Watching", "Completed", "On-Hold",
            "Dropped", "Plan to Watch"
        ],
    },
    "ratings": {
        "file": "rating_complete.csv",
        "required_columns": ["user_id", "anime_id", "rating"],
    },
    "synopsis": {
        "file": "anime_with_synopsis.csv",
        "required_columns": ["MAL_ID", "Name", "Score", "Genres", "sypnopsis"],
    },
}


# Charge un CSV et sauvegarde en parquet pour passage inter-tasks
def extract_csv(dataset_key):
    schema = EXPECTED_SCHEMAS[dataset_key]
    filepath = os.path.join(REP_SOURCE, schema["file"])

    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Fichier introuvable : {filepath}")

    df = pd.read_csv(filepath)

    output_path = os.path.join(REP_SOURCE, f"raw_{dataset_key}.parquet")
    df.to_parquet(output_path, index=False)

    return {
        "dataset": dataset_key,
        "source_file": filepath,
        "output_path": output_path,
        "rows": len(df),
        "columns": list(df.columns),
    }


# Vérifie que les colonnes obligatoires sont présentes
def validate_schema(dataset_key, extract_result):
    schema = EXPECTED_SCHEMAS[dataset_key]
    actual_columns = set(extract_result["columns"])
    required = set(schema["required_columns"])

    missing = required - actual_columns
    if missing:
        raise ValueError(f"[{dataset_key}] Colonnes manquantes : {missing}")

    return {
        "dataset": dataset_key,
        "status": "valid",
        "rows": extract_result["rows"],
        "columns_checked": len(required),
    }
