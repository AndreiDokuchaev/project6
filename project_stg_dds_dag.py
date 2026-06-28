from logging import Logger
from typing import List, Dict, Any
from datetime import datetime

from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import dict_row
from pydantic import BaseModel


class CourierDdsObj(BaseModel):
    courier_id: str
    courier_name: str
    active_from: datetime
    active_to: datetime


class CouriersStgRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_couriers(self, courier_threshold: int, limit: int) -> List[Dict[str, Any]]:
        with self._db.client().cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT id, object_id, object_value, update_ts
                FROM stg.api_couriers
                WHERE id > %(threshold)s
                ORDER BY id ASC
                LIMIT %(limit)s;
                """, {
                    "threshold": courier_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class CourierDdsRepository:
    def insert_courier(self, conn: Connection, courier: CourierDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dds.dm_couriers(courier_id, courier_name, active_from, active_to)
                VALUES (%(courier_id)s, %(courier_name)s, %(active_from)s, %(active_to)s)
                ON CONFLICT (courier_id) DO UPDATE
                SET courier_name = EXCLUDED.courier_name,
                    active_from = EXCLUDED.active_from
                """,
                {
                    "courier_id": courier.courier_id,
                    "courier_name": courier.courier_name,
                    "active_from": courier.active_from,
                    "active_to": courier.active_to
                }
            )


class CourierLoader:
    WF_KEY = "stg_to_dds_couriers_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg_repo = CouriersStgRepository(pg_dest)
        self.dds_repo = CourierDdsRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_couriers(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0, 
                    workflow_key=self.WF_KEY, 
                    workflow_settings={self.LAST_LOADED_ID_KEY: -1}
                )

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg_repo.list_couriers(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} couriers to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for courier_stg in load_queue:
                courier_data = courier_stg['object_value']
                
                courier_id = courier_data.get('_id')
                courier_name = courier_data.get('name')
                
                if not courier_id or not courier_name:
                    self.log.warning(f"Missing required fields in courier data: {courier_data}")
                    continue
                
                if isinstance(courier_name, str) and '\\u' in courier_name:
                    courier_name = courier_name.encode().decode('unicode_escape')
                
                courier_dds = CourierDdsObj(
                    courier_id=courier_id,
                    courier_name=courier_name,
                    active_from=courier_stg['update_ts'],
                    active_to=datetime(2099, 12, 31)
                )
                
                self.dds_repo.insert_courier(conn, courier_dds)
                    


            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t['id'] for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")