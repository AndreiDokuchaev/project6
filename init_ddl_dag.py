import logging

import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder

from examples.dds.dds_user_loader import UserLoader
from examples.dds.dds_restaurant_loader import RestaurantLoader
from examples.dds.dds_timestamp_loader import TimestampLoader
from examples.dds.dds_product_loader import ProductLoader
from examples.dds.dds_order_loader import OrderLoader
from examples.dds.dds_fct_loader import FctSalesLoader
from project.dags.dds.courier_loader import CourierLoader
from project.dags.dds.courier_order_loader import OrderCourierLoader

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['project', 'stg', 'dds'],
    is_paused_upon_creation=True
)
def project_stg_dds_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="users_load")
    def load_users():
        user_loader = UserLoader(dwh_pg_connect, log)
        user_loader.load_users()

    
    @task(task_id="restaurants_load")
    def load_restaurants():
        restaurant_loader = RestaurantLoader(dwh_pg_connect, log)
        restaurant_loader.load_restaurants()
    
    @task(task_id="timestamps_load")
    def load_timestamps():
        timestamp_loader = TimestampLoader(dwh_pg_connect, log)
        timestamp_loader.load_timestamps()

    @task(task_id="products_load")
    def load_products():
        product_loader = ProductLoader(dwh_pg_connect, log)
        product_loader.load_products()
    
    @task(task_id="couriers_load")
    def load_couriers():
        courier_loader = CourierLoader(dwh_pg_connect, log)
        courier_loader.load_couriers()
    
    @task(task_id="orders_load")
    def load_orders():
        order_loader = OrderLoader(dwh_pg_connect, log)
        order_loader.load_orders()
    
    @task(task_id="order_couriers_load")
    def load_order_couriers():
        order_courier_loader = OrderCourierLoader(dwh_pg_connect, log)
        order_courier_loader.load_order_couriers()   
    
    @task(task_id="fct_sales_load")
    def load_fct_sales():
        fct_sales_loader = FctSalesLoader(dwh_pg_connect, log)
        fct_sales_loader.load_fct_sales()
    
    users_load_task = load_users()
    restaurants_load_task = load_restaurants()
    timestamps_load_task = load_timestamps()
    products_load_task = load_products()
    couriers_load_task = load_couriers()
    orders_load_task = load_orders()
    order_couriers_task = load_order_couriers()
    fct_sales_task = load_fct_sales()

    [users_load_task, restaurants_load_task, timestamps_load_task, couriers_load_task] >> orders_load_task
    restaurants_load_task >> products_load_task
    orders_load_task >> order_couriers_task
    [order_couriers_task, products_load_task] >> fct_sales_task


stg_to_dds_dag = project_stg_dds_dag()