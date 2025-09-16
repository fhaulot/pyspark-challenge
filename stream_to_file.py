import websocket
import json
import os
from datetime import datetime

OUTPUT_DIR = "data_stream"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Each message will be written as a new line in a file named by date

def on_message(ws, message):
    try:
        data = json.loads(message)
        # Write each message as a line in a JSON Lines file
        filename = os.path.join(OUTPUT_DIR, f"stream_{datetime.now().strftime('%Y%m%d')}.jsonl")
        with open(filename, "a") as f:
            f.write(json.dumps(data) + "\n")
    except Exception as e:
        print(f"Error writing message: {e}\nRaw message: {message}")

def on_error(ws, error):
    print(f"WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket closed. Status: {close_status_code}, Message: {close_msg}")

def on_open(ws):
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
