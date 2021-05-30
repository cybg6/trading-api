from flask import Flask, render_template, copy_current_request_context, request, jsonify
from flask_cors import CORS
from flask_socketio import SocketIO, emit
from threading import Thread, Event
from time import sleep
from database_functions import DatabaseFunctions


app = Flask(__name__)
CORS(app)
app.config['SECRET_KEY'] = 'mysecret'
socketio = SocketIO(app, cors_allowed_origins='*')

thread = Thread()
thread_stop_event = Event()


def randomNumberGenerator(pairs, tfs):

    while not thread_stop_event.isSet():

        db = DatabaseFunctions()

        candle = db.get_last_candles_tfs( pairs, tfs )
                
        socketio.emit('candle_stream', candle )

        sleep(2)

@socketio.on('candles_stream')
def candles_stream(data):

    print('\n')
    print('connected to candles_stream')
    
    global thread

    pairs = data['pairs']
    tfs = data['tfs']

    print('thread alive: ', thread.is_alive())

    if not thread.is_alive():
        thread = socketio.start_background_task(randomNumberGenerator(pairs, tfs))



@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/historic_candles', methods=['GET'])
def get_historic():
    pair = request.args.get('pair').upper()
    tf = request.args.get('tf')

    db = DatabaseFunctions()

    historic_candles = db.get_candles_historic( pair, tf )

    return historic_candles


@app.route('/api/pairs', methods=['GET'])
def get_pairs():
    status = request.args.get('status')
    print(status)

    db = DatabaseFunctions()

    if status == "ema_5m_1h":
        pairs = db.get_pairs_ema_up()
    else:
        pairs = db.get_pairs()

    return {'pairs': pairs}


@socketio.on('connect')
def connect_socket():
 
    print('\n')
    print('socket connected!')

    # global thread

    # if not thread.is_alive():
    #     thread = socketio.start_background_task(randomNumberGenerator)


# @socketio.on('disconnect')
# def disconnect_socket():

#     print('\n')
#     print('socket disconnect!')
#     thread_stop_event.set()




@socketio.on('screener_stream')
def screener_stream(data):

    print('\n')
    print('connected to screener_stream')
    
    global thread
    tfs = [ '1w', '1d', '1h' ]
    tfs = [ '1h', '5m', '1d' ]

    print('thread alive: ', thread.is_alive())

    if not thread.is_alive():
        thread = socketio.start_background_task(randomNumberGenerator(tfs))





# @socketio.on('message')
# def handleMessage(msg):
#     print('Message: ' + msg)
#     send( msg, broadcast=True )

# @socketio.on('msg_to_server')
# def test_msg(data):
#     print('\n')
#     print(data)
#     emit( 'msg',  { 'msg': 'msg' })



if __name__ == '__main__':
    socketio.run(app)

