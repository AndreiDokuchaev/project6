from logging import Logger
from typing import List, Dict, Any

from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import dict_row


class OrderCourierRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_deliveries(self, delivery_threshold: int, limit: int) -> List[Dict[str, Any]]:
        with self._db.client().cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT id, object_id, object_value, update_ts
                FROM stg.api_deliveries
                WHERE id > %(threshold)s
                ORDER BY id ASC
                LIMIT %(limit)s;
                """, {
                    "threshold": delivery_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs

    def get_courier_id(self, courier_business_id: str) -> int:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                SELECT id 
                FROM dds.dm_couriers 
                WHERE courier_id = %(courier_business_id)s;
                """, {
                    "courier_business_id": courier_business_id
                }
            )
            result = cur.fetchone()
            return result[0] if result else None

    def get_order_id(self, order_business_key: str) -> int:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                SELECT id 
                FROM dds.dm_orders 
                WHERE order_key = %(order_business_key)s;
                """, {
                    "order_business_key": order_business_key
                }
            )
            result = cur.fetchone()
            return result[0] if result else None


class OrderCourierDdsRepository:
    def update_order_courier(self, conn: Connection, order_id: str, courier_id: int) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE dds.dm_orders 
                SET courier_id = %(courier_id)s
                WHERE order_key = %(order_id)s;
                """,
                {
                    "order_id": order_id,
                    "courier_id": courier_id
                }
            )


class OrderCourierLoader:
    WF_KEY = "stg_to_dds_order_couriers_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg_repo = OrderCourierRepository(pg_dest)
        self.dds_repo = OrderCourierDdsRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_order_couriers(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0, 
                    workflow_key=self.WF_KEY, 
                    workflow_settings={self.LAST_LOADED_ID_KEY: -1}
                )

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg_repo.list_deliveries(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} deliveries to process.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            updated_count = 0
            skipped_count = 0

            for delivery_stg in load_queue:
                delivery_data = delivery_stg['object_value']
                
                order_id = delivery_data.get('order_id')
                courier_business_id = delivery_data.get('courier_id')
                
                if not order_id or not courier_business_id:
                    self.log.warning(f"Missing order_id or courier_id in delivery: {delivery_data}")
                    skipped_count += 1
                    continue

                courier_dds_id = self.stg_repo.get_courier_id(courier_business_id)
                if not courier_dds_id:
                    self.log.warning(f"Courier with business id {courier_business_id} not found in dds.dm_couriers")
                    skipped_count += 1
                    continue

                order_dds_id = self.stg_repo.get_order_id(order_id)
                if not order_dds_id:
                    self.log.warning(f"Order with business key {order_id} not found in dds.dm_orders")
                    skipped_count += 1
                    continue

                self.dds_repo.update_order_courier(conn, order_id, courier_dds_id)
                updated_count += 1


            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t['id'] for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Updated {updated_count} orders with courier data, skipped {skipped_count}")