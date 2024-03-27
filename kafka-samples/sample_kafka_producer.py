import json

from kafka import KafkaProducer


def publish_message(kafka_producer, topic_name, key, value):
    try:
        key_str = str(key)
        key_bytes = bytes(key_str, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        kafka_producer.send(topic_name, key=key_bytes, value=value_bytes)
        kafka_producer.flush()
        print('Message published successfully.')
    except Exception as ex:
        print(type(ex))


if __name__ == '__main__':
    kafka_producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    students = [
        {
            "name": "John Smith",
            "id": 1
        }, {
            "name": "Susan Doe",
            "id": 2
        }, {
            "name": "Karen Rock",
            "id": 3
        },
        {
            "name": "Ashwin",
            "id": 4
        }, {
            "name": "Pankaj",
            "id": 5
        }, {
            "name": "Dhanasamy",
            "id": 6
        },
        {
            "name": "Ashwin",
            "id": 7
        }, {
            "name": "Pankaj",
            "id": 8
        }, {
            "name": "Dhanasamy",
            "id": 9
        },
    ]
    for student in students:
        publish_message(
            kafka_producer=kafka_producer,
            topic_name='students',
            key=student['id'],
            value=json.dumps(student)
        )
    if kafka_producer is not None:
        kafka_producer.close()