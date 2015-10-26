import time
import math
import threading
import signal
import sys
from flask import Flask
from flask import Flask, render_template
from flask.ext.socketio import SocketIO, send, emit
from pymongo.mongo_client import MongoClient
from pymongo.cursor import CursorType
from stream_tweet_listener import keywords
from threading import Thread

client = None
server = None

class TweetServer():

    def __init__(self, key, collection):
        self.key = key
        self.t = None
        self.collection = collection
        self.key_change = False

    def coordinates_data_stream(self):
        print "\ndata stream running"
        cursor = self.collection.find({"keyword" : self.key}, cursor_type=CursorType.TAILABLE)
        print "\nCount  = ", cursor.count()
        while cursor.alive and not self.key_change:
            try:
                data = cursor.next()
                data_coordinates = data['coordinates'][1]
                print data_coordinates
                socketio.emit('initialdata', data_coordinates)
            except StopIteration:
                pass

    def startThread(self, func):
        self.key_change = False
        self.t = Thread( target = func )
        self.t.daemon = True
        self.t.start()

    def stopThread(self):
        self.key_change = True
        if self.t.isAlive():
           self.t.join()

def signal_handler(signal, frame):
    print '\nYou pressed Ctrl+C!'
    sys.exit(0)

app = Flask(__name__)
socketio = SocketIO(app)

if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    client = MongoClient()
    db = client.tweetDB
    server = TweetServer("love",db.tweets)
    server.startThread(server.coordinates_data_stream)
    socketio.run(app)

@socketio.on('keywordchange')
def handle_keyword_change(keyword):
    server.stopTread()
    server.key = keyword
    server.startThread()


@app.route('/tweetmap')
def route_function():
    return 'Working'
