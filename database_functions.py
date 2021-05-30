import psycopg2
import pandas as pd
from datetime import timedelta, datetime

from time import perf_counter_ns, gmtime, strftime

class DatabaseFunctions:

    def __init__(self):
        self.host_name = 'localhost'
        self.user_name = 'postgres'
        self.user_password = 'gro32A'
        self.db_name = 'binance_data'

        try:
            self._conn = psycopg2.connect(
                host=self.host_name,
                user=self.user_name,
                password=self.user_password,
                dbname=self.db_name
            )
        except Exception as err:
            print(f"Error: '{err}'")

        self._cursor = self._conn.cursor()

    def execute_query( self, query, data = None ):
        try:
            self._cursor.execute( query, data )
            self._conn.commit()
            # print("Query successful")
        except Exception as err:
            self._conn.rollback()
            print(f"Error: '{err}'")

    def executemany_query( self, query, data ):
        try:
            self._cursor.executemany( query, data )
            self._conn.commit()
            # print("Query successful")
        except Exception as err:
            self._conn.rollback()
            print(f"Error: '{err}'")

    def read_query( self, query, data = None ):
        result = None
        try:
            self._cursor.execute( query, data )
            result = self._cursor.fetchall()
            return result
        except Exception as err:
            print(f"Error: '{err}'")

    

    def create_table_candles(self):
            qry = """CREATE TABLE candles (
                pair VARCHAR(20),
                tf VARCHAR(12),
                time integer,
                open DECIMAL( 20, 10 ),
                high DECIMAL( 20, 10 ),
                low DECIMAL( 20, 10 ),
                close DECIMAL( 20, 10 ),
                volume DECIMAL( 30, 15 ),

                close_ema20 DECIMAL( 20, 10 ), 
                volume_ema20 DECIMAL( 30, 15 ),

                close_ema_diff_percentage DECIMAL( 20, 10 ),
                price_change_percentage DECIMAL( 20, 10 ),
                volume_change_percentage DECIMAL( 30, 15 ),

                PRIMARY KEY ( pair, tf, time )
            )"""

            self.execute_query( qry )

    def create_table_pairs(self):
            # qry = "CREATE TYPE trend AS ENUM( 'down', 'up' );"
            qry = """CREATE TABLE IF NOT EXISTS pairs (
                pair VARCHAR(20) NOT NULL,
                step_size DECIMAL( 20, 10 ) NULL,
                quote_volume BIGINT,
                active BOOLEAN DEFAULT true,

                price_change_percent_24h DECIMAL( 20, 10 ),
                price_change_percent_1h DECIMAL( 20, 10 ),
                price_change_percent_5m DECIMAL( 20, 10 ),
                price_change_percent_1m DECIMAL( 20, 10 ),
                
                volume_change_percent_24h DECIMAL( 30, 15 ),
                volume_change_percent_1h DECIMAL( 30, 15 ),
                volume_change_percent_5m DECIMAL( 30, 15 ),
                volume_change_percent_1m DECIMAL( 30, 15 ),

                ema_5m_1h trend NULL,
                ema_1h_1d trend NULL,

                PRIMARY KEY ( pair )
            )"""

            self.execute_query( qry )

    def drop_table_pairs(self):
            qry = "DROP TABLE pairs"

            self.execute_query( qry )



##
## PAIRS
## 

    def upsert_pair( self, pair ):
        qry = ( "INSERT INTO pairs (pair, quote_volume) VALUES ( %(pair)s, %(quote_volume)s ) ON CONFLICT (pair) DO UPDATE SET (pair, quote_volume) = (EXCLUDED.pair, EXCLUDED.quote_volume)" )

        data = { 'pair': pair['symbol'].upper(), 'quote_volume': pair['quoteVolume'] }

        self.execute_query( qry, data )


    def update_emas_trend( self, pair, ema_5m_1h, ema_1h_1d ):
        qry = ( "UPDATE pairs SET ema_5m_1h = %(ema_5m_1h)s, ema_1h_1d = %(ema_1h_1d)s WHERE pair = %(pair)s" )

        data = { 'pair': pair , 'ema_5m_1h': ema_5m_1h, 'ema_1h_1d': ema_1h_1d }

        self.execute_query( qry, data )


    def get_active_pairs(self):
        qry = ( "SELECT pair FROM pairs WHERE active != false ORDER BY quote_volume desc" )

        pairs_tuple = self.read_query( qry )

        pairs = [tick[0] for tick in pairs_tuple ]

        return pairs


    def get_pairs_ema_up(self):
        qry = ( "SELECT pair, quote_volume FROM pairs WHERE active = true AND ema_5m_1h = 'up' order by quote_volume desc" )

        pairs_tuple = self.read_query( qry ) 
                
        pairs = [{ 'pair':tick[0], 'quoteVolume':tick[1] } for tick in pairs_tuple ]
        
        return pairs

    
    def get_pair(self, pair):
        qry = ( "SELECT * FROM pairs WHERE pair = %(pair)s" )
        data = { 'pair': pair }

        return self.read_query( qry, data )


    def update_pair_trend( self, pair ):
        tfs = {
            'tfs_5m_1h': [ '5m', '1h' ],
            'tfs_1h_1d': [ '1h', '1d' ]
        }

        trends = {}
        for trend in tfs:
            trends[trend] = {}
            for tf in tfs[trend]:
                qry = "SELECT ema20 FROM candles WHERE pair = %(pair)s AND tf = %(tf)s ORDER BY time DESC LIMIT 1"
                data = { 'pair': pair, 'tf':tf }

                ema_last_value = self.read_query( qry, data )

                if len(ema_last_value) == 0:
                    return

                trends[trend][tf] = ema_last_value[0][0]
        
        ema_5m_1h = 'down'
        ema_1h_1d = 'down'

        if trends['tfs_5m_1h']['5m'] > trends['tfs_5m_1h']['1h']:
            ema_5m_1h = 'up'
        
        if trends['tfs_1h_1d']['1h'] > trends['tfs_1h_1d']['1d']:
            ema_1h_1d = 'up'

        self.update_emas_trend( pair, ema_5m_1h, ema_1h_1d )

        # print("pair: ", self.get_pair( pair ))
          

##
## CANDLES
##

#
# INSERT

    def _insert_candles( self, candles_df ):
        qry = ( "INSERT INTO candles VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s ) ON CONFLICT (pair,tf,time) DO UPDATE SET (open, high, low, close, volume, ema20) = (EXCLUDED.open, EXCLUDED.high, EXCLUDED.low, EXCLUDED.close, EXCLUDED.volume, EXCLUDED.ema20)" )
        
        data = candles_df.values.tolist()

        self.executemany_query( qry, data )


    def upsert_candles( self, pair, tf, candles_df ):
        pair = pair.upper()
        first_ema20_value = None

        # obtener previous candle 
        prev_candle_db_df = self.get_candle_previous( pair, tf, int(candles_df.at[ 0, 'time' ]) )

        # set el primer valor de ema20, si hay previous candle usar su ema20, si no usar close de la primer candles_df
        if prev_candle_db_df.empty is False:
            first_ema20_value = prev_candle_db_df.at[ 0 ,'ema20' ]
        else:
            first_ema20_value = candles_df.at[ 0, 'close' ]  

        # parametros de ema
        length = 20 #length de ema.. no me acuerdo que era 
        k = 2 / ( length + 1 )

        # calcular emas de todas las candles
        for index, row in candles_df.iterrows():
            previous_candle_ema20 = first_ema20_value

            if index >= 1:
                previous_candle_index = index - 1
                previous_candle_ema20 = candles_df.at[ previous_candle_index, 'ema20' ]

            # present_candle_close_price = candles_df.at[ index, 'close' ]

            candles_df.at[ index, 'ema20' ] = row['close'] * k + previous_candle_ema20 * (1 - k)
        
        # print( "previous candle: " )
        # print( prev_candle_db_df )
        # print( "present candle: " )
        # print( candles_df )

        # insert candles con ema calculado
        self._insert_candles( candles_df )


#
# SELECT
# 
# asc = higher last (default), desc = higher first

    def _get_candles( self, qry, data, columns=['pair', 'tf','time', 'open', 'high', 'low', 'close', 'volume', 'ema20'] ):
        candles_db = self.read_query( qry, data )

        # print('columns: ', columns)

        candles_db_df = pd.DataFrame( candles_db, columns=columns )

        candles_db_df = candles_db_df.apply( pd.to_numeric, errors='ignore' )

        return candles_db_df

        
    def get_candles_historic( self, pair, tf, format = 'json', time = None):
        _print = False
        now = datetime.now()
        deltas = {
            '1w': 200,
            '1d': 40,
            '1h': 10,
            '5m': 2,
            '1m': 1,
        }
        td = 0

        if time == None:
            td = timedelta(weeks=deltas[tf])

        time = now - td
        time = time.timestamp()

        qry = ( "SELECT pair, tf, time, open, high, low, close, volume, ema20 FROM candles WHERE tf = %(tf)s AND pair = %(pair)s AND time > %(time)s ORDER BY time asc" )
        
        data = { 'tf':tf, 'pair':pair, 'time':time }

        candles_db_df = self._get_candles( qry, data )
        candles_db_df = candles_db_df.drop(columns=[ 'pair', 'tf', 'volume', 'ema20' ])

        emas_for_tfs = {
            '1w': [ '1w', '1d' ],
            # '1d': [ '1w' ],
            '1d': [ '1w', '1d', '1h' ],
            '1h': [ '1d', '1h', '5m' ],
            '5m': [ '1d', '1h', '5m' ],
            '1m': [ '1d', '1h', '5m', '1m' ],
        }

        emas = {}

        emas_times = candles_db_df[['time']].copy()
        
        if _print: print( tf ) 

        # get valores de los emas de diferentes timeframes, segun las candles del timeframe... vos me entedes(?
        for ema_tf in emas_for_tfs[tf]:
            if _print: print( "ema: ", ema_tf )
            t1 = perf_counter_ns()
            
            qry_ema = ( "SELECT time, ema20 FROM candles WHERE tf = %(tf)s AND pair = %(pair)s AND time > %(time)s ORDER BY time asc" )
            data_ema = { 'tf':ema_tf, 'pair':pair, 'time':time }
            columns_ema = [ 'time', 'ema20' ] #nombre de la columnas del dataframe que devuelve

            emas_tf = self._get_candles( qry_ema, data_ema, columns=columns_ema )

            t2 = perf_counter_ns()
            if _print: print(f"total klines fetch ms: {(t2 - t1) / 1000000}")
            
            t21 = perf_counter_ns()

            ema = emas_times
            
            new_ema = pd.merge( ema, emas_tf, on="time", how="left" )
            
            new_ema = new_ema.fillna( method='ffill' )
            new_ema = new_ema.fillna( method='bfill' )
            new_ema = new_ema.rename( columns={ "ema20":"value" } )                   
        
            ema = new_ema.to_dict( 'records' )

            t22 = perf_counter_ns()
            if _print: print(f"total pandas emas ms: {(t22 - t21) / 1000000}\n")

            # print(new_ema.to_string())    

            emas[ema_tf] = ema

                    
        candles_db_df = candles_db_df.to_dict( 'records' )

        response = { 
            'candles': candles_db_df, 
            'emas': emas 
        } 

        return response

    
    def get_last_candles_tfs( self, pairs, pair_tfs ):
        # historic klines
        data = {
            'pair1':  {
                '1d': {
                    # desde aca
                    'candles':[],
                    'emas': {
                        '1h': {},
                        '1w': {}
                    }
                },
                '1h': {
                    'candles':[],
                    'emas': {
                        '1h': {},
                        '5m': {}
                    }
                },
            },
            'pair2': {}
        }
        
        data = {
            'pair1':  {
                'candles': {
                    '1h':[],
                    '1d':[]
                },
                'emas': {
                    '1h':[],
                    '1d':[]
                }
            },
            'pair2': {}
        }


        pairs_data = {}

        for pair in pairs:
            pair_data = {}

            for pair_tf in pair_tfs:
                last_candle_df = self.get_candle_last_inserted( pair, pair_tf )

                # todos los emas de cada timeframe para el timeframe de la candle
                emas_for_tfs = {
                    '1w': [ '1w', '1d' ],
                    # '1d': [ '1w' ],
                    '1d': [ '1w', '1d', '1h' ],
                    '1h': [ '1d', '1h', '5m' ],
                    '5m': [ '1d', '1h', '5m' ],
                    '1m': [ '1d', '1h', '5m', '1m' ],
                }

                emas = {}

                emas_times = last_candle_df[['time']].copy()

                last_candle_df = last_candle_df.to_dict( 'records' )
                
                for ema_tf in emas_for_tfs[pair_tf]:
                
                    qry_ema = ( "SELECT time, ema20 FROM candles WHERE tf = %(tf)s AND pair = %(pair)s ORDER BY time desc LIMIT 1" )
                    data_ema = { 'tf':ema_tf, 'pair':pair }
                    columns_ema = [ 'time', 'ema20' ]
                    emas_tf = self._get_candles( qry_ema, data_ema, columns=columns_ema )

                    ema = emas_times
                    
                    ema['value'] = emas_tf['ema20']                 
                    ema = ema.to_dict( 'records' )

                    emas[ema_tf] = ema

                pair_data[pair_tf] = { 
                    'candles': last_candle_df, 
                    'emas': emas 
                }
                
            pairs_data[pair] = pair_data

        
        return pairs_data
            


    def get_candle_previous( self, pair, tf, time ):
            qry = ( "SELECT pair, tf, time, open, high, low, close, volume, ema20 FROM candles WHERE tf = %(tf)s AND pair = %(pair)s AND time < %(time)s ORDER BY time desc LIMIT 1")

            data = { "tf": tf, "pair": pair, "time": time }

            return self._get_candles( qry, data )

    def get_candle_last_inserted( self, pair, tf ):
            qry = ( "SELECT pair, tf, time, open, high, low, close, volume, ema20 FROM candles WHERE tf = %(tf)s AND pair = %(pair)s ORDER BY time desc LIMIT 1" )

            data = { "tf": tf, "pair": pair }

            return self._get_candles( qry, data )


#
# FORMAT


    

    

    def convert_to_chart_values( self, candles_df ):
        candles_ohlc_df = candles_df.drop(columns=[ 'pair', 'tf', 'volume', 'ema20' ])
        candles_ohlc_dict = candles_ohlc_df.to_dict( 'records' )

        candles_ema_df = candles_df.drop(columns=[ 'pair', 'tf', 'open', 'high', 'low', 'close', 'volume' ])
        
        candles_ema_dict = candles_ema_df.to_dict( 'records' )

        return { 
            'candles': candles_ohlc_dict, 
            'emas': candles_ema_dict 
        } 


    # def get_last_candle_dict( self, pair, tf ):
    #     last_candle_df = self.get_candle_last_inserted( pair, tf )

    #     # last_candle_df = last_candle_df.drop(columns=[ 'pair', 'tf', 'open', 'high', 'low', 'close', 'volume', 'ema20' ])

    #     last_candle_dict = last_candle_df.to_dict( 'records' )

    #     return last_candle_dict


    def get_all_emas_last_values( self, pair, tfs ):
            emas = {}
            for tf in tfs:
                qry = "SELECT ema20 FROM candles WHERE pair = %(pair)s AND tf = %(tf)s ORDER BY time DESC LIMIT 1"
                data = { 'pair': pair, 'tf':tf }

                ema_last_value = self.read_query( qry, data )

                emas[tf] = ema_last_value[0][0]

            return emas




    # tfs = [ '1m', '5m', '1h', '1d', '1w' ]

    # # print( get_last_candle( 'ethusdt', '5m') )
    # # print( get_all_emas_last_values( 'ethusdt', tfs) )

    # create_table_candles()
    # candles_db = get_candles_in_db( 'ethusdt', '1w')

