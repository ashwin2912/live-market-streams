import json
from time import sleep

from kafka import KafkaConsumer

if __name__ == '__main__':
    consumer = KafkaConsumer(
        'students',
        auto_offset_reset='earliest',
        bootstrap_servers=['localhost:9092'],
        api_version=(0, 10),
        consumer_timeout_ms=1000
    )
    for msg in consumer:
        if msg.value:
            try:
                record = json.loads(msg.value)
                employee_id = int(record['id'])
                name = record['name']
                print(f"This employee {name}")
                sleep(2)
            except json.JSONDecodeError:
                print("Not valid json:")
                print(msg)

    if consumer is not None:
        consumer.close()