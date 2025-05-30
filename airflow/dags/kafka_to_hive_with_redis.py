import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from confluent_kafka import Consumer, KafkaException,KafkaError
import avro.schema
import avro.io
import io
from pyhive import hive
import time
import redis


# Path to the Avro schema
AVRO_SCHEMA_PATH = "/opt/airflow/dags/test-schema.avsc"

REDIS_HOST = 'redis'
REDIS_PORT = 6379
MAX_WAIT_TIME = 10

# Function to consume Kafka messages and insert into Hive
def consume_kafka_to_hive_with_redis():
    # Load the Avro schema
    with open(AVRO_SCHEMA_PATH, "r") as schema_file:
        schema_str = schema_file.read()
    try:
        schema = avro.schema.parse(schema_str)
    except Exception as e:
        logging.error(f"Schema parsing failed: {e}")
        return
    # Connexion à Redis
    r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    # Connect to Hive
    conn = hive.Connection(host='hive-server', port=10000, username='hive', database='default')
    cursor = conn.cursor()

    # Create the Hive table if it doesn't exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sales (
        id STRING,
        invoice_id STRING,
        branch STRING,
        city STRING,
        customer_type STRING,
        gender STRING,
        product_line STRING,
        unit_price FLOAT,
        quantity INT,
        tax_5_percent FLOAT,
        total FLOAT,
        `date` STRING,
        time STRING,
        payment STRING,
        cogs FLOAT,
        gross_margin_percentage FLOAT,
        gross_income FLOAT,
        rating FLOAT
    ) STORED AS ORC
    """)

    # Kafka consumer configuration
    consumer_config = {
        'bootstrap.servers': 'broker:29092',
        'group.id': 'airflow_hive_consumer',
        'auto.offset.reset': 'latest',
        'max.poll.interval.ms': 1200000
    }
    consumer = Consumer(consumer_config)

    # Subscribe to the topic
    consumer.subscribe(['batch-topic'])
    metadata = consumer.list_topics(timeout=10)
    if 'batch-topic' in metadata.topics:
        logging.info(f"Topic 'batch-topic' exists with {len(metadata.topics['batch-topic'].partitions)} partitions.")
    else:
        logging.error("Topic 'batch-topic' does not exist.")


    try:
        wait_time = 0
        stop_processing = False
        while True:
            while r.get('batch_status') != 'READY':
                if wait_time < MAX_WAIT_TIME:
                    logging.info(f"Waiting for producer, elapsed time: {wait_time}s...")
                    time.sleep(1)
                    wait_time += 1
                else:
                    logging.error("Timeout reached while waiting for producer.")
                    stop_processing = True
                    break
            if stop_processing:
                break

            # Traiter les messages du lot
            buffer = []
            batch_size = 50
            for _ in range(batch_size):
                msg = consumer.poll(timeout=20)
                print(msg.value())
                if msg is None:
                    logging.info("No message retrieved from Kafka.")
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        continue
                    else:
                        raise KafkaException(msg.error())
                
                # Extract and process the Avro-encoded payload
                msg_value = msg.value()
                bytes_reader = io.BytesIO(msg_value)

                # Skip the magic byte and schema ID
                bytes_reader.read(5)  # Magic byte (1 byte) + schema ID (4 bytes)
                
                # Deserialize the message
                decoder = avro.io.BinaryDecoder(bytes_reader)
                reader = avro.io.DatumReader(schema)
                data = reader.read(decoder)

                # Debug log the consumed data
                logging.info(f"Consumed message: {data}")
                buffer.append(data)
            
            for data in buffer:
                # Prepare and execute the insertion query
                insert_query = """
                    INSERT INTO sales VALUES (
                        '{id}', '{invoice_id}', '{branch}', '{city}', '{customer_type}', '{gender}', 
                        '{product_line}', {unit_price}, {quantity}, {tax_5_percent}, {total}, 
                        '{date}', '{time}', '{payment}', {cogs}, {gross_margin_percentage}, 
                        {gross_income}, {rating}
                    )
                """
                try:
                    cursor.execute(insert_query.format(**data))
                    logging.info("Inserted message into Hive.")
                except Exception as e:
                    logging.error(f"Error inserting into Hive: {e}")
            
            # Create batch views after inserting the data
            create_sales_monthly_summary(cursor)
            create_gender_sales_summary(cursor)
            create_payment_method_sales_summary(cursor)
            create_profitability_by_product_line(cursor)
            create_product_performance_summary(cursor)
            
            # Ajouter un délai de 5 minutes avant de continuer
            logging.info("Waiting 2 minutes before processing the next batch...")
            time.sleep(120)

            # Signaler que le lot a été consommé et inséré dans Hive
            r.set('batch_status', 'DONE')
            logging.info(f"Redis batch status: {r.get('batch_status')}")
            
    except KeyboardInterrupt:
        pass
    finally:
        # Close the consumer and Hive connection
        consumer.close()
        conn.close()


def create_sales_monthly_summary(cursor):

    # Step 1: Create the sales_monthly_summary table with explicit schema
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sales_monthly_summary
        (
            product_line STRING,
            year INT,
            month INT,
            total_quantity INT,
            total_sales DOUBLE
        )
        STORED AS PARQUET
    """)
    cursor.execute("TRUNCATE TABLE sales_monthly_summary")

    # Step 2: Insert aggregated data from the sales table into the sales_monthly_summary table
    cursor.execute("""
        INSERT INTO sales_monthly_summary
        SELECT
            product_line,
            YEAR(CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(`date`, 'MM/dd/yyyy')) AS DATE)) AS year,
            MONTH(CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(`date`, 'MM/dd/yyyy')) AS DATE)) AS month,
            SUM(quantity) AS total_quantity,
            SUM(total) AS total_sales
        FROM
            sales
        GROUP BY
            product_line, 
            YEAR(CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(`date`, 'MM/dd/yyyy')) AS DATE)),
            MONTH(CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(`date`, 'MM/dd/yyyy')) AS DATE))
    """)

    logging.info("Created and populated sales_monthly_summary table")

# Create profitability_by_product_line table and insert aggregated data
def create_profitability_by_product_line(cursor):

    # Step 1: Create the profitability_by_product_line table with explicit schema
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS profitability_by_product_line
        (
            product_line STRING,
            total_sales DOUBLE,
            total_cost DOUBLE,
            total_profit DOUBLE
        )
        STORED AS PARQUET
    """)
    cursor.execute("TRUNCATE TABLE profitability_by_product_line")

    # Step 2: Insert aggregated data into the table
    cursor.execute("""
        INSERT INTO profitability_by_product_line
        SELECT
            product_line,
            SUM(total) AS total_sales,
            SUM(cogs) AS total_cost,
            SUM(total) - SUM(cogs) AS total_profit
        FROM
            sales
        GROUP BY
            product_line
    """)

    logging.info("Created and populated profitability_by_product_line table")


# Create product_performance_summary table and insert aggregated data
def create_product_performance_summary(cursor):
    
    # Step 1: Create the product_performance_summary table with explicit schema
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS product_performance_summary
        (
            product_line STRING,
            avg_margin DOUBLE,
            total_quantity_sold INT
        )
        STORED AS PARQUET
    """)
    cursor.execute("TRUNCATE TABLE product_performance_summary")

    # Step 2: Insert aggregated data into the table
    cursor.execute("""
        INSERT INTO product_performance_summary
        SELECT
            product_line,
            AVG(gross_margin_percentage) AS avg_margin,
            SUM(quantity) AS total_quantity_sold
        FROM
            sales
        GROUP BY
            product_line
    """)

    logging.info("Created and populated product_performance_summary table")


# Create payment_method_sales_summary table and insert aggregated data
def create_payment_method_sales_summary(cursor):
    
    # Step 1: Create the payment_method_sales_summary table with explicit schema
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS payment_method_sales_summary
        (
            payment STRING,
            total_sales DOUBLE,
            total_invoices INT
        )
        STORED AS PARQUET
    """)
    cursor.execute("TRUNCATE TABLE payment_method_sales_summary")

    # Step 2: Insert aggregated data into the table
    cursor.execute("""
        INSERT INTO payment_method_sales_summary
        SELECT
            payment,
            SUM(total) AS total_sales,
            COUNT(DISTINCT invoice_id) AS total_invoices
        FROM
            sales
        GROUP BY
            payment
    """)

    logging.info("Created and populated payment_method_sales_summary table")


# Create gender_sales_summary table and insert aggregated data
def create_gender_sales_summary(cursor):
    
    # Step 1: Create the gender_sales_summary table with explicit schema
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gender_sales_summary
        (
            gender STRING,
            total_sales DOUBLE,
            total_invoices INT
        )
        STORED AS PARQUET
    """)
    cursor.execute("TRUNCATE TABLE gender_sales_summary")

    # Step 2: Insert aggregated data into the table
    cursor.execute("""
        INSERT INTO gender_sales_summary
        SELECT
            gender,
            SUM(total) AS total_sales,
            COUNT(DISTINCT invoice_id) AS total_invoices
        FROM
            sales
        GROUP BY
            gender
    """)

    logging.info("Created and populated gender_sales_summary table")