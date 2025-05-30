from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from csv_to_kafka_with_redis import send_csv_to_kafka_with_redis
from kafka_to_hive_with_redis import consume_kafka_to_hive_with_redis
import threading

def run_producer_and_consumer():
    # Lancer le producteur et le consommateur simultanément
    producer_thread = threading.Thread(target=send_csv_to_kafka_with_redis)
    consumer_thread = threading.Thread(target=consume_kafka_to_hive_with_redis)
    # Démarrer les deux threads
    producer_thread.start()
    consumer_thread.start()
    # Attendre que les deux threads se terminent
    producer_thread.join()
    consumer_thread.join()

# Définir le DAG
with DAG(
    dag_id='kafka_hive_pipeline_threaded',
    default_args={'start_date': datetime(2024, 12, 2)},
    schedule_interval=None,
    max_active_runs=1,
) as dag:
    sync_task = PythonOperator(
        task_id='sync_producer_and_consumer',
        python_callable=run_producer_and_consumer,
    )
