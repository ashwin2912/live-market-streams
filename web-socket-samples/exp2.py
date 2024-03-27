from coinbase.websocket import WSClient
import time

api_key = "organizations/19da0fd2-5595-4c34-83df-fbb6f38f55ba/apiKeys/413491a7-8434-4cee-b08d-ea6f64d5c54b"
api_secret = "-----BEGIN EC PRIVATE KEY-----\nMHcCAQEEINRcEk7flo+0cmXreL/g+xmeoJcR6taAm9IFVxAaA0nSoAoGCCqGSM49\nAwEHoUQDQgAEzaPKs2FgwfQJcW6xTV6graHJqDfzjZTB43NJYj3qoc9NUW6/1JWy\nSKhtISqPDcDU3mWKhiKrL8ZxBDcTchtcIg==\n-----END EC PRIVATE KEY-----\n"

def on_message(msg):
    print(type(msg))

print("Entered")
client = WSClient(api_key=api_key, api_secret=api_secret, on_message=on_message)
client.open()
client.subscribe(product_ids=["BTC-USD", "ETH-USD"], channels=["ticker", "heartbeats"])

# wait 10 secxonds
time.sleep(10)