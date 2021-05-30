import config_binance
# from binance.client import AsyncClient
from binance import BinanceSocketManager, AsyncClient, Client
from binance.enums import *

from database_functions import DatabaseFunctions as DB 
# import database_functions_module as db

import pandas as pd
import numpy as np

# from playsound import playsound
from time import perf_counter_ns
import datetime

import asyncio

from decimal import *

###############

#
# HISTORIC KLINES

async def get_binance_pair_historic_candles ( task_num, clientAsync, pair_queue, tfs, refresh=False ):
    inicio = "16 august 2017"
    tfs_date_first_default = {
        '1w': inicio,
        '1d': inicio,
        '1h': "6 months ago",
        '5m': "1 months ago",
        '1m': "1 weeks ago",
    }
    
    try:
        while True:
            db =  DB()
            
            pair = await pair_queue.get()

            for tf in tfs:
                date_first = tfs_date_first_default[tf]

                last_candle_db = db.get_candle_last_inserted( pair, tf )

                # si hay candles de ese pair y tf en la db, si no usar el default date
                if last_candle_db.empty is False and refresh == False:
                    last_candle_db_time_UTC = datetime.datetime.utcfromtimestamp( last_candle_db.at[ 0 ,'time'] )
                    date_first = last_candle_db_time_UTC.strftime("%d %B %Y %H:%M %p")

                # get historical kline from binance                
                candles_list_raw = await clientAsync.get_historical_klines( pair.upper(), tf, date_first)
                                
                if len(candles_list_raw) > 0:
                    # convertir a numpy array con valores float
                    candles_np = np.array( candles_list_raw, dtype=np.float64 )

                    # slice valores que no me importan y pasar a list, dejar solo time, open, high, low, close, volume y volver a list
                    candles_list = candles_np[:,:6].tolist()

                    candles_df = candles_list_to_df( pair, tf, candles_list )

                    if len( candles_df ) > 0:
                        db.upsert_candles( pair, tf, candles_df )
    except Exception:
        print(Exception)
        # raise
    finally:
        print(clientAsync.response.headers['x-mbx-used-weight'])
        pair_queue.task_done()   


async def task(name, work_queue):

    # timer = Timer(text=f"Task {name} elapsed time: {{:.1f}}")
    while not work_queue.empty():
        pair = await work_queue.get()

        print(f"timer {name} - {pair} awaiting...")
        await asyncio.sleep(1.5)
        print(f"timer {name} - {pair} ready")

        if name == "four": print("\n")



async def prueba( pairs, tfs ):

    # weight = len(pairs) * len(tfs)
    # print(f"total pairs: {len(pairs) }")
    # print(f"total tfs: {len(tfs) }")
    # print(f"total weight: { weight }")

    # if weight < 1199:

    pair_queue = asyncio.Queue()

    for pair in pairs:
        pair_queue.put_nowait(pair)
    
    t21 = perf_counter_ns()

    clientAsync = await AsyncClient.create(config_binance.API_KEY, config_binance.API_SECRET)
    
    tasks = []

    for i in range(80):
        task = asyncio.create_task( get_binance_pair_historic_candles ( i + 1, clientAsync, pair_queue, tfs ))
        tasks.append(task)

    # await asyncio.gather(
    #     *[get_binance_pair_historic_candles ( task_num, clientAsync, pair_queue, tfs ) for task_num in tasks]
    #     # asyncio.create_task( get_binance_pair_historic_candles( clientAsync, pair_queue, tfs )),
    # )
    try:
        await pair_queue.join()
    except TimeoutError:
        print( "Timeout Error")
        # break
    except Exception as e:
        print( "Exception get candles")
        print(e)
    else:
        print("all good")

    print("tasks listo")

    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)
        
    await clientAsync.close_connection()
    
    t22 = perf_counter_ns()
    print(f"total s: {(t22 - t21) / 1000000000}")

    # else:
    #     print("weight too damn high!!!")



#
# WEBSOCKET

async def connect_ws_binance( pairs, tfs ):
    klines = []
    
    # global bm

    for pair in pairs:
        for tf in tfs:
            # ['btcusdt@kline_1m', 'btcusdt@kline_5m', 'btcusdt@kline_1h']
            kline_str = "{}@kline_{}".format( pair.lower(), tf ) 

            klines.append(kline_str)

    clientAsync = await AsyncClient.create()

    bm = BinanceSocketManager( clientAsync )

    ts = bm.multiplex_socket( klines )
    
    print(klines)
    print("websocket start")

    async with ts as tscm:
        while True:
            res = await tscm.recv()
            process_ws_message(res)

    await clientAsync.close_connection()


def process_ws_message( msg ):    
    db = DB() 
    if msg['data']['e'] == 'error':
        print("websocket closed")
    else:

        #time, open, high, low, close, volume
        candle_data= msg['data']['k']
        time = int(candle_data['t']) 
        open = float(candle_data['o'])
        high = float(candle_data['h'])
        low = float(candle_data['l'])
        close = float(candle_data['c'])
        volume = float(candle_data['v'])

        candle_list = [[ time, open, high, low, close, volume ]]

        pair = candle_data['s'].upper()
        tf = candle_data['i'] #interval
        
        #isclosed = candle_data['x']
        print("new candle: ",pair)
        
        if pair == "ETHUSDT":
            print("new candle") 

        candle_df = candles_list_to_df( pair, tf, candle_list )

        db.upsert_candles( pair, tf, candle_df )

        if tf == '5m':
            db.update_pair_trend( pair )


# HELPERS
#

# format candles_list = time, open, high, low, close, volume
def candles_list_to_df( pair, tf, candles_list ):
    # convertir a df

    # print( candles_list )

    candles_df = pd.DataFrame( candles_list, columns=[ 'time', 'open', 'high', 'low', 'close', 'volume' ] )
    
    # pasar time de milisecond to seconds
    candles_df['time'] = candles_df['time'] / 1000

    # convertir time de float a int
    candles_df = candles_df.astype({'time': np.int64})

    candles_df['pair'] = pair
    candles_df['tf'] = tf
    candles_df['ema20'] = 0.0 # pone float64

    # rearrange columns
    candles_df = candles_df[[ 'pair', 'tf', 'time', 'open', 'high', 'low', 'close', 'volume', 'ema20' ]]

    return candles_df

# INIT
#

# def create_tables():
#     db.create_table_candles()
#     db.create_table_pairs()




def get_pairs_vol_usdt():
    db =  DB()    
    
    client = Client.create(config_binance.API_KEY, config_binance.API_SECRET)

    tickers = client.get_ticker()

    tickers_usdt = [{ 'symbol':tick['symbol'], 'quoteVolume':int(float(tick['quoteVolume'])), 'priceChangePercent':float(tick['priceChangePercent']) } for tick in tickers if "USDT" in tick['symbol']]

    # for t in tickers_usdt:
    #     print( t )

    # print(len(tickers_usdt))

    for pair_obj in tickers_usdt:
        db.upsert_pair( pair_obj )




def update_pairs_historic_candles( pairs, tfs ):
    loop = asyncio.get_event_loop()
    loop.run_until_complete( get_binance_pair_historic_candles( pairs, tfs ) )
    


def connect_ws_pairs( pairs, tfs ):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(connect_ws_binance( pairs, tfs ))
        


#
# INIT
tfs_historic = [ '1w', '1d', '1h', '5m', '1m' ]
tfs_ws = [  '1h', '5m' ]
# db = DB()
# bm = None





def main():
    while True:
        try:

            pairs = DB().get_active_pairs()

            loop = asyncio.get_event_loop()
            loop.run_until_complete(prueba( pairs, tfs_ws ))


            # pairs = ['ETHUSDT']

            # update_pairs_historic_candles( pairs, tfs_historic )
        except Exception as e:
            print (e)
            pass
        except TimeoutError:
            print( "Timeout Error")
            break
        except KeyboardInterrupt:
            print( "keyboard interruption")
            break
        else:
            break


if __name__ == "__main__":
    main()

    # update_pairs_historic_candles( tfs_historic )

    # print(client.get_symbol_info('venusdt'))


    # get_pairs_vol_usdt()


    # pairs = [ 'BCCUSDT' ]
    # tfs = [ '5m' ]
    # get_binance_pair_historic_candles ( pairs, tfs )



    # t21 = perf_counter_ns()

    # for pair in pairs:
    #     for tf in tfs:
    #         db.get_candles_historic( pair, tf )

    # t22 = perf_counter_ns()
    # print(f"total time ms: {(t22 - t21) / 1000000}\n")








############################


# def _symbol_step_size( pair ):
#     info = client.get_symbol_info( pair )
#     filters = info['filters']
#     lot_size = list(filter(lambda f: f['filterType'] == "LOT_SIZE", filters))
#     step_size = Decimal(lot_size[0]['stepSize']).normalize()
#     return step_size


# def check_alarms( pair, close ):
#     DBcon = DB()
#     alarms = DBcon.get_price_lines( pair, 'alarm%' )
    
#     if alarms:    
#         global alarm_active, th
#         if alarm_active == False:
#             for alarm in alarms:
#                 al_type = alarm[0]
#                 al_price = alarm[1]            
#                 if (al_type == "alarm_up" and close >= al_price) or (al_type == "alarm_down" and close <= al_price):
#                         print("alarm activated")
#                         alarm_active = True  
#                         th = threading.Thread(target=sound_alarm, args=[al_type, al_price])
#                         th.start()
#                 else:
#                     alarm_active = False
#     else:
#         alarm_active = False

            
# def sound_alarm( alarm_type, alarm_price ):
#     global alarm_active
#     for _ in range(1000):
#         if alarm_active == True:
#             if alarm_type == "alarm_up":
#                 # playsound('./sounds/up.mp3')
#                 time.sleep(0.5)
#             elif alarm_type == "alarm_down":                
#                 # playsound('./sounds/down.mp3')
#                 time.sleep(1)
#         else:
#             return


# def close_ws( ws ):
#     ws.close()


# def process_ws_message_account(msg):
#     global ws_account_data
#     ws_account_data = msg

# def connect_ws_account( ):

#     ws_account = BinanceSocketManager( client, user_timeout=60 )

#     ws_account.start_user_socket(process_ws_message_account)
#     ws_account.start()


# def get_position_relative_to_ema ( pair, close ):
#     DBcon = DB()
#     emas = DBcon.get_all_emas_last_values( pair )
#     pos = {}

#     for ema in emas:
#         tf = ema[0]
#         ema_price = ema[1]
#         if close >= ema_price:
#             pos[tf] = 'up'
#         elif close <= ema_price:
#             pos[tf] = 'down'
    
#     return pos
