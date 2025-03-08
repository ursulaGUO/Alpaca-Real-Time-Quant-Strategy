#!/usr/bin/env python3

import socket
import threading
import json
import argparse
import sys
import time
import datetime
import sqlite3
import pandas as pd

DB_FILE = "data/trade_data.db"
START_DATE = "2025-03-03"


class ThreadedServer(object):
    def __init__(self, host, opt):
        self.environment = {}
        self.environment['NoMode'] = {'points': 0}
        self.environment['Occupancy'] = {'occupancy': 0, 'points': 0}
        self.host = host
        self.port = opt.port
        self.opt = opt
        self.state = self.environment[opt.mode if opt.mode else 'NoMode']
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        self.lock = threading.Lock()

    def listen(self):
        """Start the TCP server and handle client connections."""
        self.sock.listen(5)
        while True:
            client, address = self.sock.accept()
            client.settimeout(500)
            threading.Thread(target=self.listenToClient, args=(client, address)).start()
            threading.Thread(target=self.sendStreamToClient, args=(client,)).start()

    def handle_client_answer(self, obj):
        """Handles client responses (if any mode-specific logic applies)."""
        if self.opt.mode is not None and self.opt.mode == 'Occupancy':
            if 'Occupancy' not in obj:
                return
            self.lock.acquire()
            if self.state['occupancy'] == int(obj['Occupancy']):
                self.state['points'] += 1
            self.lock.release()
        return

    def listenToClient(self, client, address):
        """Receives data from the client and processes it."""
        size = 1024
        while True:
            try:
                data = client.recv(size).decode()
                if data:
                    a = json.loads(data.rstrip('\n\r '))
                    self.handle_client_answer(a)
                else:
                    print('Client disconnected')
                    return False
            except:
                print('Client closed the connection')
                print("Unexpected error:", sys.exc_info()[0])
                client.close()
                return False

    def handleCustomData(self, buffer):
        """Modify data before sending (if needed)."""
        if self.opt.mode is not None and self.opt.mode == 'Occupancy':
            self.lock.acquire()
            buffer['date'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.state['occupancy'] = int(buffer['Occupancy'])
            buffer['Occupancy'] = -1
            self.lock.release()

    
    def sendStreamToClient(self, client):
        """Continuously streams data from the database to the client."""
        while True:
            data = self.fetchDBData()

            if not data:
                print("No data fetched. Sleeping before retrying...")
                time.sleep(5)  # Wait before retrying
                continue  # Try again

            for row in data:
                row["timestamp"] = row["timestamp"].strftime("%Y-%m-%d %H:%M:%S")  # Convert Timestamp to string

                print(f"Sending: {row}")  # Debugging print

                try:
                    client.send((self.convertStringToJSON(row) + '\n').encode('utf-8'))
                    time.sleep(self.opt.interval)  # Simulate real-time streaming
                except:
                    print('Client disconnected or end of stream')
                    return  # Exit loop but keep the server running




    def convertStringToJSON(self, data):
        """Converts a dictionary to a JSON string."""
        return json.dumps(data)

    def fetchDBData(self):
        """Fetch data from SQLite `merged_data` table from '2025-03-03' onward."""
        conn = sqlite3.connect(DB_FILE)
        query = f"SELECT * FROM merged_data WHERE timestamp >= '{START_DATE}' ORDER BY timestamp ASC"
        df = pd.read_sql(query, conn, parse_dates=["timestamp"])
        conn.close()

        if not df.empty:
            print(df.head())  # Print first few rows

        return df.to_dict(orient="records")




if __name__ == "__main__":
    parser = argparse.ArgumentParser(usage='usage: tcp_server -p port [-m]')
    parser.add_argument("-m", "--mode", action="store", dest="mode")
    parser.add_argument("-p", "--port", action="store", dest="port", type=int)
    parser.add_argument("-t", "--time-interval", action="store",
                        dest="interval", type=float, default=0.5)

    opt = parser.parse_args()
    if not opt.port:
        parser.error('Port not given')
    ThreadedServer('127.0.0.1', opt).listen()
