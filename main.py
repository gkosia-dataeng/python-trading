""" Start the program to ingest and visualize volume analysis data from Binance API.
    Usage:  
        main.py --symbol=<symbol> --interval=<interval> --price_zone_size=<price_zone_size>
        main.py (-h | --help)


"""
import os
from time import sleep
from docopt import docopt
from dotenv import load_dotenv
import logging
from threading import Thread

from binance_utils.BinanceAPIManager import BinanceAPIManager
from duckdb_utils.DatabaseManager  import DatabaseManager
from webapp.app import WebApp

logging.basicConfig(level=logging.INFO)


if __name__ == '__main__':

    # get binance api credentials
    load_dotenv()
    binance_key = os.getenv('binance-api-key')
    binance_secret = os.getenv('binance-api-secret')

    # parse arguments
    args = docopt(__doc__)
    symbol = args['--symbol']
    interval = args['--interval']
    price_zone_size = args['--price_zone_size']
    logging.debug(f"Running app with symbol={symbol}, interval={interval}, price_zone_size={price_zone_size}")

    # initiate database
    db = DatabaseManager(interval, price_zone_size)
    
    # initiate the binance client
    binance_client = BinanceAPIManager(binance_key, binance_secret, symbol, db)

    loading_data_thread = Thread(target=binance_client.start_stream, daemon=True, name='Get data form binance')
    loading_data_thread.start()
    

    # initiate the app
    WebApp.set_db_conn(db)
    app = WebApp.initiate_app()
    app.run_server(port=4050)