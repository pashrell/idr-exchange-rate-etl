from datetime import date, datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

from kurs_common import CURRENCIES, DBT, DBT_ARGS, PREP, extract_range


@dag(
    dag_id="kurs_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="0 8 * * 1-5",  # hari kerja, 08:00
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["etl", "kurs", "dbt"],
)
def kurs_pipeline():
    @task
    def extract_and_load(base: str, ds=None):
        # ambil 7 hari ke belakang agar akhir pekan/libur tetap tertutup
        end = date.fromisoformat(ds)
        return extract_range(base, end - timedelta(days=7), end)

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{PREP}{DBT} deps {DBT_ARGS} && {DBT} run {DBT_ARGS}",
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{PREP}{DBT} deps {DBT_ARGS} && {DBT} test {DBT_ARGS}",
    )

    extract_and_load.expand(base=CURRENCIES) >> dbt_run >> dbt_test


kurs_pipeline()
