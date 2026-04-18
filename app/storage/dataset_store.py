from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable

import pandas as pd

from app.config import get_settings


def get_connection() -> sqlite3.Connection:
    settings = get_settings()
    Path(settings.db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(settings.db_path)
    conn.row_factory = sqlite3.Row
    return conn


def reset_dataset_tables() -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS raw_transactions")
    cur.execute("DROP TABLE IF EXISTS enriched_transactions")

    conn.commit()
    conn.close()


def init_dataset_tables() -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_transactions (
            dataset_index INTEGER PRIMARY KEY,
            transaction_id TEXT,
            user_id INTEGER,
            account_age_days INTEGER,
            total_transactions_user INTEGER,
            avg_amount_user REAL,
            amount REAL,
            country TEXT,
            bin_country TEXT,
            channel TEXT,
            merchant_category TEXT,
            promo_used TEXT,
            avs_match TEXT,
            cvv_result TEXT,
            three_ds_flag TEXT,
            transaction_time TEXT,
            shipping_distance_km REAL,
            is_fraud INTEGER
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS enriched_transactions (
            dataset_index INTEGER PRIMARY KEY,
            transaction_id TEXT,
            user_id INTEGER,
            account_age_days INTEGER,
            total_transactions_user INTEGER,
            avg_amount_user REAL,
            amount REAL,
            country TEXT,
            bin_country TEXT,
            channel TEXT,
            merchant_category TEXT,
            promo_used TEXT,
            avs_match TEXT,
            cvv_result TEXT,
            three_ds_flag TEXT,
            transaction_time TEXT,
            shipping_distance_km REAL,
            is_fraud INTEGER,
            risk_score REAL,
            risk_level TEXT,
            recommended_action TEXT
        )
        """
    )

    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_enriched_user_id ON enriched_transactions(user_id)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_enriched_action ON enriched_transactions(recommended_action)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_enriched_risk_score ON enriched_transactions(risk_score DESC)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_enriched_merchant_category ON enriched_transactions(merchant_category)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_enriched_time ON enriched_transactions(transaction_time)"
    )

    conn.commit()
    conn.close()


def ingest_raw_dataset(csv_path: str) -> int:
    df = pd.read_csv(csv_path).reset_index().rename(columns={"index": "dataset_index"})
    df["transaction_time"] = pd.to_datetime(df["transaction_time"], errors="coerce").astype(str)

    init_dataset_tables()

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("DELETE FROM raw_transactions")

    rows = [
        (
            int(row["dataset_index"]),
            str(row["transaction_id"]),
            int(row["user_id"]),
            int(row["account_age_days"]),
            int(row["total_transactions_user"]),
            float(row["avg_amount_user"]),
            float(row["amount"]),
            str(row["country"]),
            str(row["bin_country"]),
            str(row["channel"]),
            str(row["merchant_category"]),
            str(row["promo_used"]),
            str(row["avs_match"]),
            str(row["cvv_result"]),
            str(row["three_ds_flag"]),
            str(row["transaction_time"]),
            float(row["shipping_distance_km"]),
            int(row["is_fraud"]),
        )
        for _, row in df.iterrows()
    ]

    cur.executemany(
        """
        INSERT INTO raw_transactions (
            dataset_index,
            transaction_id,
            user_id,
            account_age_days,
            total_transactions_user,
            avg_amount_user,
            amount,
            country,
            bin_country,
            channel,
            merchant_category,
            promo_used,
            avs_match,
            cvv_result,
            three_ds_flag,
            transaction_time,
            shipping_distance_km,
            is_fraud
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )

    conn.commit()
    conn.close()
    return len(rows)


def load_raw_transactions_dataframe() -> pd.DataFrame:
    conn = get_connection()
    df = pd.read_sql_query(
        "SELECT * FROM raw_transactions ORDER BY dataset_index ASC",
        conn,
    )
    conn.close()
    return df


def replace_enriched_transactions(rows: Iterable[dict]) -> int:
    init_dataset_tables()

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("DELETE FROM enriched_transactions")

    materialized = list(rows)

    cur.executemany(
        """
        INSERT INTO enriched_transactions (
            dataset_index,
            transaction_id,
            user_id,
            account_age_days,
            total_transactions_user,
            avg_amount_user,
            amount,
            country,
            bin_country,
            channel,
            merchant_category,
            promo_used,
            avs_match,
            cvv_result,
            three_ds_flag,
            transaction_time,
            shipping_distance_km,
            is_fraud,
            risk_score,
            risk_level,
            recommended_action
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                int(row["dataset_index"]),
                str(row["transaction_id"]),
                int(row["user_id"]),
                int(row["account_age_days"]),
                int(row["total_transactions_user"]),
                float(row["avg_amount_user"]),
                float(row["amount"]),
                str(row["country"]),
                str(row["bin_country"]),
                str(row["channel"]),
                str(row["merchant_category"]),
                str(row["promo_used"]),
                str(row["avs_match"]),
                str(row["cvv_result"]),
                str(row["three_ds_flag"]),
                str(row["transaction_time"]),
                float(row["shipping_distance_km"]),
                int(row["is_fraud"]),
                float(row["risk_score"]),
                str(row["risk_level"]),
                str(row["recommended_action"]),
            )
            for row in materialized
        ],
    )

    conn.commit()
    conn.close()
    return len(materialized)
    
def upsert_enriched_transactions_batch(rows: Iterable[dict]) -> int:
    init_dataset_tables()

    conn = get_connection()
    cur = conn.cursor()

    materialized = list(rows)
    if not materialized:
        conn.close()
        return 0

    cur.executemany(
        """
        INSERT OR REPLACE INTO enriched_transactions (
            dataset_index,
            transaction_id,
            user_id,
            account_age_days,
            total_transactions_user,
            avg_amount_user,
            amount,
            country,
            bin_country,
            channel,
            merchant_category,
            promo_used,
            avs_match,
            cvv_result,
            three_ds_flag,
            transaction_time,
            shipping_distance_km,
            is_fraud,
            risk_score,
            risk_level,
            recommended_action
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                int(row["dataset_index"]),
                str(row["transaction_id"]),
                int(row["user_id"]),
                int(row["account_age_days"]),
                int(row["total_transactions_user"]),
                float(row["avg_amount_user"]),
                float(row["amount"]),
                str(row["country"]),
                str(row["bin_country"]),
                str(row["channel"]),
                str(row["merchant_category"]),
                str(row["promo_used"]),
                str(row["avs_match"]),
                str(row["cvv_result"]),
                str(row["three_ds_flag"]),
                str(row["transaction_time"]),
                float(row["shipping_distance_km"]),
                int(row["is_fraud"]),
                float(row["risk_score"]),
                str(row["risk_level"]),
                str(row["recommended_action"]),
            )
            for row in materialized
        ],
    )

    conn.commit()
    conn.close()
    return len(materialized)
    
def clear_enriched_transactions() -> None:
    init_dataset_tables()
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM enriched_transactions")
    conn.commit()
    conn.close()