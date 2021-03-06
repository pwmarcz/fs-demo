"""
A simple framework for UNIX socket server and client, exchanging JSON messages.

The server spawns a new thread for every client connection.
"""
import itertools
import json
import logging
import socketserver
import sys
from contextlib import closing
import socket
from threading import Lock, Thread, current_thread
from typing import List, Dict


class Conn(socketserver.BaseRequestHandler):
    """
    A single server connection.
    """
    counter = itertools.count()

    def __init__(self, request, client_address, server):
        self.ipc_server: 'IpcServer' = server.ipc_server
        self.id = next(self.counter)
        self.lock = Lock()
        super().__init__(request, client_address, server)

    def handle(self) -> None:
        self.ipc_server.add_conn(self)
        try:
            with closing(self.request.makefile()) as f:
                for line in f:
                    logging.info('server receive from %s: %s', self.id, line.rstrip())
                    data = json.loads(line)
                    self.ipc_server.handle_message(self, data)
        except Exception:
            logging.exception('error in Handler')
        finally:
            with self.lock:
                self.request.close()
            self.ipc_server.remove_conn(self)

    def send(self, data):
        line = json.dumps(data) + '\n'
        logging.info('server send to %s: %s', self.id, line.rstrip())
        with self.lock:
            self.request.sendall(line.encode())

    def __repr__(self):
        return f'<Conn: {self.id}>'


class SocketServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    pass


class IpcServer:
    def __init__(self, path):
        self.path = path
        self.socket_server = SocketServer(path, Conn)
        self.socket_server.ipc_server = self
        self.conns: List[Conn] = []
        self.methods: Dict[str, callable] = {}

    def add_conn(self, conn: Conn):
        logging.info('server add_conn: %s', conn)
        self.conns.append(conn)

    def remove_conn(self, conn: Conn):
        logging.info('server remove_conn: %s', conn)
        self.conns.remove(conn)

    def handle_message(self, conn: Conn, data):
        method, args = data[0], data[1:]
        if method not in self.methods:
            raise KeyError(f'unknown method: {method}')
        self.methods[method](conn, *args)

    def run(self):
        logging.info('server listening at %s', self.path)
        self.socket_server.serve_forever()


class IpcClient:
    def __init__(self, path):
        self.path = path
        self.conn = None
        self.thread = None
        self.lock = Lock()
        self.methods: Dict[str, callable] = {}

    def start(self):
        self.conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.conn.connect(self.path)
        logging.info('client connected to %s', self.path)
        self.thread = Thread(target=self._run)
        self.thread.start()

    def stop(self):
        if self.conn:
            self.conn.shutdown(socket.SHUT_RDWR)
            self.conn.close()
        self.thread.join()

    def handle_message(self, data):
        method, args = data[0], data[1:]
        if method not in self.methods:
            raise KeyError(f'unknown method: {method}')
        self.methods[method](*args)

    def _run(self):
        try:
            with closing(self.conn.makefile()) as f:
                for line in f:
                    logging.info('client receive: %s', line.rstrip())
                    data = json.loads(line)
                    self.handle_message(data)
        except Exception:
            logging.exception('error in IPC')
            sys.exit(1)

    def send(self, data):
        line = json.dumps(data) + '\n'
        logging.info('client send: %s', line.rstrip())
        with self.lock:
            self.conn.sendall(line.encode())
