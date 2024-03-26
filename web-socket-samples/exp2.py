from coinbase.websocket import WSClient
import time

api_key=""
api_secret=""

def on_message(msg):
    print(msg)

print("Entered")
client = WSClient(api_key=api_key, api_secret=api_secret, on_message=on_message)
client.open()
client.subscribe(product_ids=["BTC-USD", "ETH-USD"], channels=["ticker", "heartbeats"])

# wait 10 seconds
time.sleep(10)