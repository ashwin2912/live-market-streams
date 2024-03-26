import websockets
import json
import websocket
import asyncio
import certifi
import ssl

def send_message(subscribe_message):
    ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ssl_context.load_verify_locations(certifi.where())
    url = "wss://ws-feed-public.sandbox.exchange.coinbase.com"
    ws = websocket.create_connection(url, ssl=ssl_context)
    ws.send(json.dumps(subscribe_message))
    ws.close()



async def subscribe(url):
    print("Entered subscribe")
    subscribe_message = {
        "type": "subscribe",
        "product_ids": ["ETH-USD", "ETH-EUR"],
        "channels": [
            "level2",
            "heartbeat",
            {
                "name": "ticker",
                "product_ids": ["ETH-BTC", "ETH-USD"]
            }
        ]
    }


    async with websockets.connect(url) as websocket:
        # Send subscribe message
        #await websocket.send(json.dumps(subscribe_message))
        ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_context.load_verify_locations(certifi.where())
        url = "wss://ws-feed-public.sandbox.exchange.coinbase.com"
        ws = websocket.create_connection(url, ssl=ssl_context)
        await ws.send(json.dumps(subscribe_message))
        print(f"> Sent: {subscribe_message}")

        # Wait for messages from the server
        async for message in websocket:
            print(f"< Received: {message}")

# Replace 'your_websocket_url' with the actual WebSocket URL
url = "wss://ws-feed-public.sandbox.exchange.coinbase.com"

asyncio.get_event_loop().run_until_complete(subscribe(url))

#if __name__ == '__main__':
    #subscribe(url=url)

