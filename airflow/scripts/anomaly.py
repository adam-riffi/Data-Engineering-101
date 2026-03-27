# AniData Lab — Détection d'anomalies sur les ratings

import os
import pandas as pd
import numpy as np


REP_IMPORTS = "/opt/airflow/data/imports"   # CSV fallback
REP_EXPORTS = "/opt/airflow/data/exports"   # parquets, gold, anomaly outputs


def load_ratings():
    path = os.path.join(REP_EXPORTS, "raw_ratings.parquet")
    if not os.path.exists(path):
        path = os.path.join(REP_IMPORTS, "rating_complete.csv")
    if path.endswith(".parquet"):
        return pd.read_parquet(path)
    return pd.read_csv(path)


# Users avec un nombre déraisonnable de ratings (> seuil)
def detect_spam_users(min_ratings=5000):
    df = load_ratings()
    user_counts = df.groupby("user_id").size()
    spam_users = user_counts[user_counts > min_ratings]

    result = {
        "total_users": int(user_counts.shape[0]),
        "spam_users": int(len(spam_users)),
        "threshold": min_ratings,
        "max_ratings": int(user_counts.max()),
        "user_ids": spam_users.index.tolist()[:50],
    }

    flagged = df[df["user_id"].isin(spam_users.index)].copy()
    flagged["anomaly_type"] = "spam_volume"
    flagged.to_parquet(os.path.join(REP_EXPORTS, "anomalies_spam.parquet"), index=False)

    return result


# Users qui donnent toujours la même note (std = 0, min 10 ratings)
def detect_mono_raters():
    df = load_ratings()
    user_stats = df.groupby("user_id")["rating"].agg(["std", "count"])
    mono = user_stats[(user_stats["std"] == 0) & (user_stats["count"] >= 10)]

    result = {
        "mono_raters": int(len(mono)),
        "min_ratings_threshold": 10,
        "user_ids": mono.index.tolist()[:50],
    }

    flagged = df[df["user_id"].isin(mono.index)].copy()
    flagged["anomaly_type"] = "mono_rater"
    flagged.to_parquet(os.path.join(REP_EXPORTS, "anomalies_mono.parquet"), index=False)

    return result


# Animes avec un ratio anormalement élevé de notes à 1 (review bombing)
def detect_suspicious_ratings():
    gold_path = os.path.join(REP_EXPORTS, "anime_gold_latest.csv")
    if not os.path.exists(gold_path):
        return {"status": "skipped", "reason": "gold dataset not found"}

    df = pd.read_csv(gold_path)

    score_cols = [f"Score-{i}" for i in range(1, 11)]
    for col in score_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["total_votes"] = df[score_cols].sum(axis=1)
    df["score1_ratio"] = np.where(
        df["total_votes"] > 100,
        df["Score-1"] / df["total_votes"],
        0
    )

    # ratio > 10% de notes à 1 = suspect
    bombed = df[df["score1_ratio"] > 0.10][["MAL_ID", "Name", "Score", "score1_ratio", "total_votes"]]
    bombed = bombed.sort_values("score1_ratio", ascending=False)

    bombed.to_csv(os.path.join(REP_EXPORTS, "anomalies_review_bombing.csv"), index=False)

    return {
        "total_checked": int(len(df)),
        "review_bombed": int(len(bombed)),
        "worst_offenders": bombed.head(10).to_dict("records") if len(bombed) > 0 else [],
    }


# Compile un rapport des anomalies détectées
def anomaly_report():
    report_lines = ["=" * 60, "RAPPORT D'ANOMALIES — AniData Lab", "=" * 60]

    for name, path in [
        ("Spam users", "anomalies_spam.parquet"),
        ("Mono-raters", "anomalies_mono.parquet"),
        ("Review bombing", "anomalies_review_bombing.csv"),
    ]:
        full = os.path.join(REP_EXPORTS, path)
        if os.path.exists(full):
            if path.endswith(".parquet"):
                count = len(pd.read_parquet(full))
            else:
                count = len(pd.read_csv(full))
            report_lines.append(f"{name}: {count} lignes flaggées")
        else:
            report_lines.append(f"{name}: non exécuté")

    report = "\n".join(report_lines)
    report_path = os.path.join(REP_EXPORTS, "anomaly_report.txt")
    with open(report_path, "w") as f:
        f.write(report)

    return {"report_path": report_path, "content": report}
