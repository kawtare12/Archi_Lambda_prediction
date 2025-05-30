import uuid
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from confluent_kafka import Producer, KafkaError
import json
import logging
import time

# Arguments par défaut pour le DAG
default_args = {
    'owner': 'admin',
    'start_date': datetime(2024, 11, 29, 00, 30)
}

# Fonction de retour d'appel pour la livraison des messages
def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Message delivery failed: {err}")
    else:
        logging.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Fonction pour traiter le fichier sales.csv et publier sur Kafka
def process_sales_and_publish():
    try:
        file_path = '/opt/airflow/data/sales.csv'

        # Lire tout le fichier CSV
        df = pd.read_csv(file_path)

        # Initialiser le producteur Kafka
        kafka_config = {
            'bootstrap.servers': 'broker:29092',
            'acks': 'all',
            'retries': 5,
            'delivery.timeout.ms': 120000
        }

        producer = Producer(kafka_config)

        # Publier les données par blocs de 50 lignes
        for i in range(0, len(df), 50):
            block = df.iloc[i:i + 50]  # Extraire 50 lignes
            for _, row in block.iterrows():
                data = {
                    'id': str(uuid.uuid4()),
                    'invoice_id': row['Invoice ID'],
                    'branch': row['Branch'],
                    'city': row['City'],
                    'customer_type': row['Customer type'],
                    'gender': row['Gender'],
                    'product_line': row['Product line'],
                    'unit_price': row['Unit price'],
                    'quantity': row['Quantity'],
                    'tax_5_percent': row['Tax 5%'],
                    'total': row['Total'],
                    'date': row['Date'],
                    'time': row['Time'],
                    'payment': row['Payment'],
                    'cogs': row['cogs'],
                    'gross_margin_percentage': row['gross margin percentage'],
                    'gross_income': row['gross income'],
                    'rating': row['Rating']
                }
                producer.produce('stream-topic', json.dumps(data).encode('utf-8'), callback=delivery_report)

            # Attendre 30 secondes entre chaque publication de bloc
            time.sleep(30)

        # Assurer que tous les messages ont été envoyés
        producer.flush()
        logging.info("Toutes les données ont été publiées avec succès.")
    
    except KafkaError as e:
        logging.error(f"Une erreur Kafka est survenue : {e}")
    except Exception as e:
        logging.error(f"Une erreur est survenue : {e}")

# Définir le DAG
with DAG('dagappi',
         default_args=default_args,
         schedule=None,
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='process_task_publish_dagappi',
        python_callable=process_sales_and_publish
    )