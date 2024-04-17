from coinbase.websocket import WSClient
import time
import json
from kafka import KafkaProducer
import ast



kafka_producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
api_key = "organizations/19da0fd2-5595-4c34-83df-fbb6f38f55ba/apiKeys/413491a7-8434-4cee-b08d-ea6f64d5c54b"
api_secret = "-----BEGIN EC PRIVATE KEY-----\nMHcCAQEEINRcEk7flo+0cmXreL/g+xmeoJcR6taAm9IFVxAaA0nSoAoGCCqGSM49\nAwEHoUQDQgAEzaPKs2FgwfQJcW6xTV6graHJqDfzjZTB43NJYj3qoc9NUW6/1JWy\nSKhtISqPDcDU3mWKhiKrL8ZxBDcTchtcIg==\n-----END EC PRIVATE KEY-----\n"

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
    client.subscribe(product_ids=["BTC-USD"], channels=["level2","heartbeats"])
    
    time.sleep(10)
    if kafka_producer is not None:
        kafka_producer.close()


