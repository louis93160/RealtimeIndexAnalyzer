from kafka import KafkaConsumer
import json

def main():
    # Configuration du consommateur Kafka
    consumer = KafkaConsumer(
        'stock-data',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='stock-data-group'
    )

    try:
        for message in consumer:
            data = message.value
            print(data)
    except Exception as e:
        print(f"Une erreur s'est produite: {e}")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
