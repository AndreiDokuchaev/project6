from logging import Logger
from typing import List
from datetime import date

from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class CourierLedgerDdsObj(BaseModel):
    courier_id: str
    courier_name: str
    settlement_year: int
    settlement_month: int
    orders_count: int
    orders_total_sum: float
    rate_avg: float
    order_processing_fee: float
    courier_order_sum: float
    courier_tips_sum: float
    courier_reward_sum: float


class CourierLedgerRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def get_courier_ledger_data(self, settlement_year: int, settlement_month: int) -> List[CourierLedgerDdsObj]:
        with self._db.client().cursor(row_factory=class_row(CourierLedgerDdsObj)) as cur:
            cur.execute(
                """
                WITH order_data AS (
                    SELECT 
                        dc.courier_id,
                        dc.courier_name,
                        EXTRACT(YEAR FROM dt.ts) AS settlement_year,
                        EXTRACT(MONTH FROM dt.ts) AS settlement_month,
                        COUNT(DISTINCT dmo.id) AS orders_count,
                        SUM(fps.total_sum) AS orders_total_sum,
                        AVG(delivery.rate) AS rate_avg,
                        SUM(delivery.tip_sum) AS courier_tips_sum
                    FROM dds.dm_orders dmo
                    INNER JOIN dds.dm_couriers dc ON dmo.courier_id = dc.id
                    INNER JOIN dds.dm_timestamps dt ON dmo.timestamp_id = dt.id
                    INNER JOIN dds.fct_product_sales fps ON dmo.id = fps.order_id
                    INNER JOIN (
                        SELECT 
                            object_value->>'order_id' as order_id,
                            (object_value->>'rate')::numeric as rate,
                            (object_value->>'tip_sum')::numeric as tip_sum
                        FROM stg.api_deliveries
                    ) delivery ON dmo.order_key = delivery.order_id
                    WHERE dmo.order_status = 'CLOSED'
                        --AND EXTRACT(YEAR FROM dt.ts) = %(settlement_year)s
                        --AND EXTRACT(MONTH FROM dt.ts) = %(settlement_month)s
                    GROUP BY dc.courier_id, dc.courier_name, settlement_year, settlement_month
                ),
                calculated_data AS (
                    SELECT
                        courier_id,
                        courier_name,
                        settlement_year,
                        settlement_month,
                        orders_count,
                        orders_total_sum,
                        rate_avg,
                        orders_total_sum * 0.25 AS order_processing_fee,
                        CASE 
                            WHEN rate_avg < 4 THEN GREATEST(orders_total_sum * 0.05, 100 * orders_count)
                            WHEN rate_avg >= 4 AND rate_avg < 4.5 THEN GREATEST(orders_total_sum * 0.07, 150 * orders_count)
                            WHEN rate_avg >= 4.5 AND rate_avg < 4.9 THEN GREATEST(orders_total_sum * 0.08, 175 * orders_count)
                            ELSE GREATEST(orders_total_sum * 0.1, 200 * orders_count)
                        END AS courier_order_sum,
                        courier_tips_sum
                    FROM order_data
                )
                SELECT
                    courier_id,
                    courier_name,
                    settlement_year::integer,
                    settlement_month::integer,
                    orders_count,
                    orders_total_sum,
                    rate_avg,
                    order_processing_fee,
                    courier_order_sum,
                    courier_tips_sum,
                    courier_order_sum + courier_tips_sum * 0.95 AS courier_reward_sum
                FROM calculated_data
                """, {
                    "settlement_year": settlement_year,
                    "settlement_month": settlement_month
                }
            )
            objs = cur.fetchall()
        return objs


class CourierLedgerCdmRepository:
    def insert_courier_ledger(self, conn: Connection, ledger: CourierLedgerDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO cdm.dm_courier_ledger(
                    courier_id, courier_name, settlement_year, settlement_month,
                    orders_count, orders_total_sum, rate_avg, order_processing_fee,
                    courier_order_sum, courier_tips_sum, courier_reward_sum
                )
                VALUES (%(courier_id)s, %(courier_name)s, %(settlement_year)s, %(settlement_month)s,
                        %(orders_count)s, %(orders_total_sum)s, %(rate_avg)s, %(order_processing_fee)s,
                        %(courier_order_sum)s, %(courier_tips_sum)s, %(courier_reward_sum)s);

                """,
                {
                    "courier_id": ledger.courier_id,
                    "courier_name": ledger.courier_name,
                    "settlement_year": ledger.settlement_year,
                    "settlement_month": ledger.settlement_month,
                    "orders_count": ledger.orders_count,
                    "orders_total_sum": ledger.orders_total_sum,
                    "rate_avg": ledger.rate_avg,
                    "order_processing_fee": ledger.order_processing_fee,
                    "courier_order_sum": ledger.courier_order_sum,
                    "courier_tips_sum": ledger.courier_tips_sum,
                    "courier_reward_sum": ledger.courier_reward_sum
                }
            )


class CourierLedgerLoader:
    WF_KEY = "dds_to_cdm_courier_ledger_workflow"
    LAST_LOADED_MONTH_KEY = "last_loaded_month"

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.dds_repo = CourierLedgerRepository(pg_dest)
        self.cdm_repo = CourierLedgerCdmRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_courier_ledger(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0, 
                    workflow_key=self.WF_KEY, 
                    workflow_settings={self.LAST_LOADED_MONTH_KEY: '2022-01'}
                )

            last_loaded_month_str = wf_setting.workflow_settings[self.LAST_LOADED_MONTH_KEY]
            last_loaded_year, last_loaded_month = map(int, last_loaded_month_str.split('-'))

            current_date = date.today()
            processing_year = current_date.year
            processing_month = current_date.month - 1
            if processing_month == 0:
                processing_year -= 1
                processing_month = 12

            self.log.info(f"Processing courier ledger for {processing_year}-{processing_month:02d}")

            ledger_data = self.dds_repo.get_courier_ledger_data(processing_year, processing_month)
            self.log.info(f"Found {len(ledger_data)} courier ledger records to load.")

            if not ledger_data:
                self.log.info("No courier ledger data found for the period.")
                return

            for ledger in ledger_data:
                self.cdm_repo.insert_courier_ledger(conn, ledger)

            wf_setting.workflow_settings[self.LAST_LOADED_MONTH_KEY] = f"{processing_year}-{processing_month:02d}"
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Courier ledger load finished for {processing_year}-{processing_month:02d}")
            self.log.info(f"Processed {len(ledger_data)} records")