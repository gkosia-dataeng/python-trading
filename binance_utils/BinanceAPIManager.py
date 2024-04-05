from binance import Client
import websocket, json
import logging

class BinanceAPIManager:

    def __init__(self, binance_key, binance_secret, symbol, db):
        self.__client = Client(binance_key,binance_secret)
        self.__db = db
        self.__symbol = symbol

    
    def on_message(self, ws, message):
        logging.debug(f"Received massage: {message}")
        json_message = json.loads(message)
        self.__db.append_aggregated_trade(json_message)



    def start_stream(self):
        socket_conn = f"wss://stream.binance.com:9443/ws/{self.__symbol}@aggTrade"    
        logging.debug(f"Connecting to {socket_conn}")
        ws = websocket.WebSocketApp(socket_conn, on_message=self.on_message)

        ws.run_forever()

