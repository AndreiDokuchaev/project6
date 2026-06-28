import logging

import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder

from project.dags.cdm.cdm_courier_ledger_loader import CourierLedgerLoader

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0 2 10 * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['project', 'cdm', 'courier_ledger'],
    is_paused_upon_creation=True
)
def project_cdm_courier_ledger_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="courier_ledger_load")
    def load_courier_ledger():
        ledger_loader = CourierLedgerLoader(dwh_pg_connect, log)
        ledger_loader.load_courier_ledger()

    courier_ledger_task = load_courier_ledger()

    courier_ledger_task


cdm_courier_ledger_dag = project_cdm_courier_ledger_dag()