import uuid
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
import logging
import redis
import time

REDIS_HOST = 'redis'
REDIS_PORT = 6379

# Configuration Kafka et Schema Registry
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"  # Nom d'hôte pour Schema Registry
BROKER_URL = "broker:29092"  # Nom d'hôte pour le broker Kafka

# Charger le schéma Avro
SCHEMA_PATH = "/opt/airflow/dags/test-schema.avsc"  # Assurez-vous que ce fichier existe dans le conteneur
with open(SCHEMA_PATH, 'r') as schema_file:
    schema_str = schema_file.read()

def send_csv_to_kafka_with_redis():
    try:
        # Connexion à Redis
        r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

        # Initializing Schema Registry client
        schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})

        # Create Avro serializer
        avro_serializer = AvroSerializer(schema_registry_client, schema_str)

        # Kafka producer config
        producer_config = {
            'bootstrap.servers': BROKER_URL,
            'key.serializer': StringSerializer('utf_8'),
            'value.serializer': avro_serializer,
            'max.poll.interval.ms': 1200000
        }
        producer = SerializingProducer(producer_config)

        # Load CSV data
        csv_path = "/opt/airflow/data/sales.csv"
        df = pd.read_csv(csv_path)

        # Debug: Log the loaded CSV data
        logging.info(f"Loaded CSV Data:\n{df.head()}")

        # Publish each row to Kafka
        topic = "batch-topic"
        batch_size = 50
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i + batch_size]  # Slice the batch
            for _, row in batch.iterrows():
                data = {
                    'id': str(uuid.uuid4()),  # ID unique pour chaque enregistrement
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
                # Set missing values to None
                for field in data:
                    if pd.isnull(data[field]):
                        data[field] = None

                # Debug: Log the row data before sending
                logging.info(f"Row data before serialization: {data}")

                # Produce the message to Kafka
                try:
                    producer.produce(topic=topic, key=data['id'], value=data)
                except Exception as e:
                    logging.error(f"Error producing message: {e}")

            # Ensure all messages are sent
            producer.flush()
            logging.info(f"Batch {i // batch_size + 1} envoyé à Kafka.")    
            time.sleep(20)
            # Mettre à jour Redis pour signaler que le lot est prêt
            r.set('batch_status', 'READY')
            logging.info(f"Redis batch status: {r.get('batch_status')}")

            # Attendre que le consommateur termine ce lot
            while r.get('batch_status') != 'DONE':
                time.sleep(1)
            r.set('batch_status', 'READY')
    except Exception as e:
        logging.error(f"Error in Kafka task: {e}")



