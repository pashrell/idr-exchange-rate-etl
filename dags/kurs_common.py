"""Fungsi dan konstanta bersama untuk pipeline harian dan pipeline backfill."""
from datetime import date, timedelta

import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook

CURRENCIES = ["USD", "EUR", "SGD", "JPY", "AUD", "CNY"]
API_URL = "https://api.frankfurter.dev/v1"

DBT = "/opt/airflow/dbt_venv/bin/dbt"
# Salin project ke /tmp agar dbt bebas menulis target/logs/packages
# tanpa masalah permission pada folder yang di-mount dari host.
PREP = (
    "rm -rf /tmp/dbt_project && cp -r /opt/airflow/dbt_project /tmp/dbt_project "
    "&& cd /tmp/dbt_project && "
)
DBT_ARGS = "--profiles-dir /tmp/dbt_project"


def load_rows(rows):
    """Upsert ke raw.exchange_rates (idempoten: aman dijalankan ulang)."""
    if not rows:
        return 0
    PostgresHook(postgres_conn_id="warehouse").insert_rows(
        table="raw.exchange_rates",
        rows=rows,
        target_fields=["rate_date", "base_currency", "target_currency", "rate"],
        replace=True,
        replace_index=["rate_date", "base_currency", "target_currency"],
    )
    return len(rows)


def extract_range(base: str, start: date, end: date, chunk_days: int = 60) -> int:
    """Ambil kurs base->IDR untuk rentang tanggal, dipecah per `chunk_days` hari.

    Pemecahan ini penting: untuk rentang panjang, API dapat mengembalikan data
    yang diagregasi (mingguan), bukan harian. Chunk pendek menjaga data tetap harian.
    """
    total, cur = 0, start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=chunk_days - 1), end)
        resp = requests.get(
            f"{API_URL}/{cur}..{chunk_end}",
            params={"base": base, "symbols": "IDR"},
            timeout=60,
        )
        if resp.status_code != 404:  # 404 = tidak ada data di rentang itu
            resp.raise_for_status()
            rates = resp.json().get("rates", {})
            total += load_rows([(d, base, "IDR", v["IDR"]) for d, v in rates.items()])
        cur = chunk_end + timedelta(days=1)
    return total
