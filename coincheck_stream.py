import websocket
import json


def on_message(ws, message):
    try:
        data = json.loads(message)
        print(json.dumps(data, indent=2))
    except Exception as e:
        print(f"Error parsing message: {e}\nRaw message: {message}")

def on_error(ws, error):
    print(f"WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket closed. Status: {close_status_code}, Message: {close_msg}")

def on_open(ws):
    # Subscribe to unconfirmed Bitcoin transactions
    subscribe_message = json.dumps({"op": "unconfirmed_sub"})
    ws.send(subscribe_message)
    print("Subscribed to unconfirmed Bitcoin transactions.")

if __name__ == "__main__":
    ws_url = "wss://ws.blockchain.info/inv"
    ws = websocket.WebSocketApp(ws_url,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.run_forever()
