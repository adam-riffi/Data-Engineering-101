import os
import re
import numpy as np
import pandas as pd


REP_SOURCE = "/opt/airflow/data"

SYNOPSIS_PLACEHOLDER = "No synopsis information has been added to this title."


def load_raw(dataset_key):
    path = os.path.join(REP_SOURCE, f"raw_{dataset_key}.parquet")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Parquet introuvable : {path}. Avez-vous lancé le DAG Extract ?")
    return pd.read_parquet(path)


def clean_anime():
    df = load_raw("anime")
    df = df.replace("Unknown", np.nan)

    df["Score"] = pd.to_numeric(df["Score"], errors="coerce")
    df["Episodes"] = pd.to_numeric(df["Episodes"], errors="coerce")
    df["Ranked"] = pd.to_numeric(df["Ranked"], errors="coerce")
    for i in range(1, 11):
        df[f"Score-{i}"] = pd.to_numeric(df[f"Score-{i}"], errors="coerce")

    for col in ["Name", "English name", "Japanese name"]:
        if col in df.columns:
            df[col] = df[col].str.strip()

    df = df.drop_duplicates(subset=["MAL_ID"])

    df.to_parquet(os.path.join(REP_SOURCE, "clean_anime.parquet"), index=False)
    return {"rows": len(df), "nulls": int(df.isnull().sum().sum())}


def clean_synopsis():
    df = load_raw("synopsis")
    if "sypnopsis" in df.columns:
        df = df.rename(columns={"sypnopsis": "Synopsis"})
    df["Synopsis"] = df["Synopsis"].str.strip()
    df["Synopsis"] = df["Synopsis"].replace(SYNOPSIS_PLACEHOLDER, np.nan)
    df = df.drop_duplicates(subset=["MAL_ID"])
    df.to_parquet(os.path.join(REP_SOURCE, "clean_synopsis.parquet"), index=False)
    return {"rows": len(df)}


def clean_ratings():
    df = load_raw("ratings")
    df = df.drop_duplicates(subset=["user_id", "anime_id"])
    df = df[df["rating"].between(1, 10)]
    df.to_parquet(os.path.join(REP_SOURCE, "clean_ratings.parquet"), index=False)
    return {"rows": len(df)}


def merge_datasets():
    anime = pd.read_parquet(os.path.join(REP_SOURCE, "clean_anime.parquet"))
    synopsis = pd.read_parquet(os.path.join(REP_SOURCE, "clean_synopsis.parquet"))

    gold = anime.merge(synopsis[["MAL_ID", "Synopsis"]], on="MAL_ID", how="left")
    gold.to_parquet(os.path.join(REP_SOURCE, "merged.parquet"), index=False)
    return {"rows": len(gold), "with_synopsis": int(gold["Synopsis"].notna().sum())}


def _extract_year_from_aired(aired_series):
    def parse_year(val):
        if pd.isna(val):
            return np.nan
        match = re.search(r'\b(19|20)\d{2}\b', str(val))
        return int(match.group()) if match else np.nan
    return aired_series.apply(parse_year)


def feature_engineering():
    df = pd.read_parquet(os.path.join(REP_SOURCE, "merged.parquet"))

    # weighted score bayésien
    C = df["Score"].mean()
    m = df["Members"].quantile(0.25)
    df["weighted_score"] = (df["Members"] * df["Score"] + m * C) / (df["Members"] + m)

    # drop ratio
    total = df["Completed"] + df["Dropped"]
    df["drop_ratio"] = np.where(total > 0, df["Dropped"] / total, 0.0)
    df["drop_ratio"] = df["drop_ratio"].round(4)

    # studio tier
    studio_counts = df["Studios"].dropna().str.split(", ").explode().value_counts()
    top = set(studio_counts.head(20).index)
    mid = set(studio_counts.head(100).index) - top

    def tier(s):
        if pd.isna(s):
            return "unknown"
        main = s.split(", ")[0]
        if main in top:
            return "top"
        if main in mid:
            return "mid"
        return "small"

    df["studio_tier"] = df["Studios"].apply(tier)

    # duration en minutes
    hours = df["Duration"].str.extract(r"(\d+)\s*hr")[0].astype(float).fillna(0)
    mins = df["Duration"].str.extract(r"(\d+)\s*min")[0].astype(float).fillna(0)
    df["duration_min"] = hours * 60 + mins
    df.loc[df["Duration"].isna(), "duration_min"] = np.nan

    # main_genre — premier genre de la liste
    df["main_genre"] = df["Genres"].dropna().str.split(", ").str[0]

    # main_studio — premier studio de la liste
    df["main_studio"] = df["Studios"].dropna().str.split(", ").str[0]

    # year et decade — extraits de la colonne Aired
    if "Aired" in df.columns:
        df["year"] = _extract_year_from_aired(df["Aired"])
        df["decade"] = (df["year"] // 10 * 10).where(df["year"].notna())
    else:
        df["year"] = np.nan
        df["decade"] = np.nan

    # engagement_ratio — Favorites / Members
    df["engagement_ratio"] = np.where(
        df["Members"] > 0,
        (df["Favorites"] / df["Members"]).round(4),
        0.0
    )

    # score_category
    def score_cat(s):
        if pd.isna(s):
            return "non noté"
        if s < 4:
            return "mauvais"
        if s < 6:
            return "moyen"
        if s < 7.5:
            return "bon"
        if s < 9:
            return "très bon"
        return "excellent"

    df["score_category"] = df["Score"].apply(score_cat)

    # is_outlier — score < 4 OU (drop_ratio > 0.6 ET Members > 500)
    df["is_outlier"] = (
        (df["Score"] < 4) |
        ((df["drop_ratio"] > 0.6) & (df["Members"] > 500))
    )

    assert df["weighted_score"].dropna().between(0, 10).all(), "weighted_score hors bornes [0, 10]"
    assert df["drop_ratio"].between(0, 1).all(), "drop_ratio hors bornes [0, 1]"

    df.to_parquet(os.path.join(REP_SOURCE, "featured.parquet"), index=False)
    return {
        "rows": len(df),
        "new_features": [
            "weighted_score", "drop_ratio", "studio_tier", "duration_min",
            "main_genre", "main_studio", "year", "decade",
            "engagement_ratio", "score_category", "is_outlier"
        ],
    }


def get_next_version():
    files = [f for f in os.listdir(REP_SOURCE) if f.startswith("anime_gold_v") and f.endswith(".csv")]
    if not files:
        return 1
    versions = []
    for f in files:
        try:
            v = int(f.replace("anime_gold_v", "").replace(".csv", ""))
            versions.append(v)
        except ValueError:
            pass
    return max(versions) + 1 if versions else 1


def check_gold_exists():
    files = [f for f in os.listdir(REP_SOURCE) if f.startswith("anime_gold_v") and f.endswith(".csv")]
    if files:
        return "export_gold_versioned"
    return "export_gold_first"


def export_gold(versioned=False):
    df = pd.read_parquet(os.path.join(REP_SOURCE, "featured.parquet"))

    if versioned:
        v = get_next_version()
    else:
        v = 1

    csv_path = os.path.join(REP_SOURCE, f"anime_gold_v{v}.csv")
    json_path = os.path.join(REP_SOURCE, f"anime_gold_v{v}.json")

    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    df.to_json(json_path, orient="records", force_ascii=False)

    latest_csv = os.path.join(REP_SOURCE, "anime_gold_latest.csv")
    latest_json = os.path.join(REP_SOURCE, "anime_gold_latest.json")
    for link, target in [(latest_csv, csv_path), (latest_json, json_path)]:
        if os.path.islink(link) or os.path.exists(link):
            os.remove(link)
        os.symlink(target, link)

    return {"version": v, "csv": csv_path, "json": json_path, "rows": len(df)}