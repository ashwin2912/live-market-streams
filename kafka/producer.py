from coinbase.websocket import WSClient
import time
import json
from kafka import KafkaProducer
import ast


kafka_producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))

def on_message(msg):
    print(msg)
    message_dict = ast.literal_eval(msg)
    publish_message(kafka_producer, 'socket-messages',message_dict["sequence_num"], json.dumps(message_dict))

# wait 10 secxonds
time.sleep(10)

def publish_message(kafka_producer, topic_name, key, value):
    try:
        key_bytes = bytes(str(key),encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        kafka_producer.send(topic_name, key=key_bytes, value=value_bytes)
        kafka_producer.flush()
        print('Message published successfully.')
    except Exception as ex:
        print(str(ex))


if __name__ == '__main__':
    print("Entered")
    client = WSClient(api_key=api_key, api_secret=api_secret, on_message=on_message)
    client.open()
    client.subscribe(product_ids=["BTC-USD", "ETH-USD"], channels=["ticker", "heartbeats"])
    
    time.sleep(10)
    if kafka_producer is not None:
        kafka_producer.close()


