from confluent_kafka import Producer

def test_kafka_connection():
    kafka_config = {
        'bootstrap.servers': 'localhost:9092',  # Make sure this matches your broker's address
        'acks': 'all',
        'retries': 5
    }
    producer = Producer(kafka_config)
    try:
        producer.produce('api', key='test', value='test message')
        producer.flush()
        print("Test message sent successfully!")
    except Exception as e:
        print(f"Failed to send test message: {e}")

# Run the test function
if __name__ == '__main__':
    test_kafka_connection()
