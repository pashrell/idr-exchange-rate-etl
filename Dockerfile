FROM apache/airflow:2.10.5
# dbt dipasang di virtualenv terpisah agar tidak bentrok dengan dependency Airflow.
# dbt-core WAJIB di-pin: tanpa itu pip menarik dbt-core 2.x yang belum mendukung Postgres.
RUN python -m venv /opt/airflow/dbt_venv \
 && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir \
    "dbt-core==1.8.9" "dbt-postgres==1.8.2"
