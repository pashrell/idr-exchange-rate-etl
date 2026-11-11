from datetime import date, datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

from kurs_common import CURRENCIES, DBT, DBT_ARGS, PREP, extract_range


@dag(
    dag_id="kurs_backfill",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # hanya dijalankan manual (Trigger DAG w/ config)
    catchup=False,
    params={
        "start_date": Param("2025-01-01", type="string", description="Format YYYY-MM-DD"),
        "end_date": Param("", type="string", description="Kosongkan = hari ini"),
    },
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["etl", "kurs", "backfill"],
)
def kurs_backfill():
    @task
    def backfill(base: str, params=None):
        start = date.fromisoformat(params["start_date"])
        end = date.fromisoformat(params["end_date"]) if params["end_date"] else date.today()
        return extract_range(base, start, end)

    # --full-refresh: model incremental hanya melihat data baru, sedangkan backfill
    # menyisipkan data lama, jadi fact table harus dibangun ulang dari awal.
    dbt_full_refresh = BashOperator(
        task_id="dbt_full_refresh",
        bash_command=f"{PREP}{DBT} deps {DBT_ARGS} && {DBT} run --full-refresh {DBT_ARGS}",
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{PREP}{DBT} deps {DBT_ARGS} && {DBT} test {DBT_ARGS}",
    )

    backfill.expand(base=CURRENCIES) >> dbt_full_refresh >> dbt_test


kurs_backfill()
