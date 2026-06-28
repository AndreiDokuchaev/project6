import logging
import os

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from project.dags.init_ddl_dag.ddl_init import InitDdl
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False, 
    tags=['sprint5', 'ddl', 'project'],
    is_paused_upon_creation=True
)
def sprint5_project_init_ddl_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    ddl_path = Variable.get("PROJECT_DDL_FILES_PATH")

    @task(task_id="ddl_init")
    def ddl_init():
        rest_loader = InitDdl(dwh_pg_connect, log)
        log.info(os.getcwd())
        rest_loader.init_ddl(ddl_path)

    init_ddl = ddl_init()

    init_ddl


project_init_ddl_dag = sprint5_project_init_ddl_dag()
