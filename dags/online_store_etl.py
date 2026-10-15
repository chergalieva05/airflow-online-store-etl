from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable
import pandas as pd

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='online_store_etl',
    default_args=default_args,
    description='ETL пайплайн для обработки заказов интернет-магазина',
    schedule='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['etl', 'online_store'],
)
def online_store_etl():

    @task()
    def extrect() -> list[dict]:
        # Читаем переменную, созданную в Admin -> Variables
        store_name = Variable.get("store_name")
        print(f"Обрабатываем магазин: {store_name}")
        
        # Сырые данные заказов
        raw_orders = [
            {"order_id": 101, "customer_id": 1, "total_cost": 15000, "status": "completed"},
            {"order_id": 102, "customer_id": 2, "total_cost": 23000, "status": "completed"},
            {"order_id": 103, "customer_id": 1, "total_cost": 8000, "status": "cancelled"},
            {"order_id": 104, "customer_id": 3, "total_cost": 42000, "status": "completed"},
        ]
        return raw_orders

    @task()
    def transform(raw_orders: list[dict]) -> list[dict]:
        df = pd.DataFrame(raw_orders)
        df_clean = df[df['status'] == 'completed'].copy()
        df_clean['is_high_value'] = df_clean['total_cost'] > 20000
        print(f"Успешно отфильтровано записей: {len(df_clean)}")
        return df_clean.to_dict(orient='records')

    @task()
    def load(transformed_orders: list[dict]):
        print("--- ЗАГРУЗКА ДАННЫХ В ХРАНИЛИЩЕ (подключение my_postgres) ---")
        for order in transformed_orders:
            print(f"Сохранен заказ №{order['order_id']} на сумму {order['total_cost']}. Высокий чек: {order['is_high_value']}")
        print(f"Всего успешно загружено записей: {len(transformed_orders)}")

    orders_data = extrect()
    transformed_data = transform(orders_data)
    load(transformed_data)

online_store_etl_dag = online_store_etl()