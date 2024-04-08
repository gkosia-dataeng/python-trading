import duckdb
from datetime import datetime
import logging
from decimal import Decimal
import numpy
import pandas

class DatabaseManager:

    def __init__(self, interval, price_zone_size):
        self.__interval = int(interval)
        self.__price_zone_size = int(price_zone_size)
        self.__conn = duckdb.connect(':memory:')
        self.initialize_database_schema()
    
    
    def initialize_database_schema(self):
        self.__conn.execute("CREATE TABLE aggregated_trades (price_zone INT, time_zone TIMESTAMP, buy_volume decimal(18,8), sell_volume decimal(18,8), total_volume decimal(18,8), CONSTRAINT pk PRIMARY KEY (price_zone, time_zone))")


    
    def append_aggregated_trade(self, message):
        # {"e":"aggTrade","E":1712325202406,"s":"BTCUSDT","a":2954790092,"p":"67459.36000000","q":"0.00045000","f":3533917325,"l":3533917325,"T":1712325202400,"m":true,"M":true}
        price_zone= int(float(message['p'])) - (int(float(message['p']))%self.__price_zone_size)

        dt_object = datetime.fromtimestamp(int(message['E'])/1000)
        rounded_minutes = round(dt_object.minute / self.__interval) * self.__interval
        rounded_dt_object = dt_object.replace(minute=rounded_minutes, second=0, microsecond=0)
        time_zone= rounded_dt_object

        volume = Decimal(message['q']).quantize(Decimal('0.00000001'))

        buy_volume= volume if message['m'] else 0
        sell_volume= volume if not message['m'] else 0
        total_volume= volume
        
        logging.debug(f"Inserting into database: price_zone={price_zone}, time_zone={time_zone}, buy_volume={buy_volume}, sell_volume={sell_volume}, total_volume={total_volume}")
        self.__conn.execute(f"""INSERT INTO aggregated_trades VALUES ({price_zone}, '{time_zone}', {buy_volume}, {sell_volume}, {total_volume})
                                ON CONFLICT (price_zone, time_zone)
                                DO UPDATE 
                                SET 
                                    buy_volume = buy_volume + {buy_volume}
                                   ,sell_volume = sell_volume + {sell_volume}
                                   ,total_volume = total_volume + {total_volume}
                            """)
        
    

    def print_latest_data(self):
        result = self.__conn.execute("SELECT * FROM aggregated_trades order by time_zone desc limit 10")

        # Fetch all rows from the result
        rows = result.fetchall()

        # Print the table
        print("Table: sample_table")
        for row in rows:
            print(row[0], "|", row[1], "|", row[2], "|", row[3], "|", row[4])


    def get_byprice_bytime_as_df(self) :
        raw_df =  self.__conn.cursor().query("SELECT * FROM aggregated_trades where CAST(time_zone AS TIMESTAMP WITH TIME ZONE) >= (now() - INTERVAL '60 minutes')").to_df()
        raw_df = raw_df.pivot_table(index='price_zone', columns='time_zone',values=['buy_volume', 'sell_volume', 'total_volume'], aggfunc='sum' )
        print(raw_df)
        return raw_df
