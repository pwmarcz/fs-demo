import json
import logging
import socketserver
import sys
from contextlib import closing
import socket
from threading import Lock, Thread
from typing import List, Dict


class Conn(socketserver.BaseRequestHandler):
    def __init__(self, request, client_address, server):
        self.ipc_server: 'IpcServer' = server.ipc_server
        self.lock = Lock()
        super().__init__(request, client_address, server)

    def handle(self) -> None:
        self.ipc_server.add_conn(self)
        try:
            with closing(self.request.makefile()) as f:
                for line in f:
                    logging.info('receive: %s', line.rstrip())
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
        logging.info('send: %s', line.rstrip())
        with self.lock:
            self.request.sendall(line.encode())


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
        logging.info('add_conn: %s', conn)
        self.conns.append(conn)

    def remove_conn(self, conn: Conn):
        logging.info('remove_conn: %s', conn)
        self.conns.remove(conn)

    def handle_message(self, conn: Conn, data):
        method, args = data[0], data[1:]
        if method not in self.methods:
            raise KeyError(f'unknown method: {method}')
        self.methods[method](conn, *args)

    def run(self):
        logging.info('listening at %s', self.path)
        self.socket_server.serve_forever()


class IpcClient:
    def __init__(self, path):
        self.path = path
        self.conn = None
        self.thread = None
        self.lock = Lock()

    def start(self):
        self.conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.conn.connect(self.path)
        logging.info('connected to %s', self.path)
        Thread(target=self._run, daemon=True).start()

    def handle_message(self, data):
        raise NotImplementedError()

    def _run(self):
        try:
            with closing(self.conn.makefile()) as f:
                for line in f:
                    logging.info('receive: %s', line.rstrip())
                    data = json.loads(line)
                    self.handle_message(data)
        except Exception:
            logging.exception('error in IPC')
            sys.exit(1)

    def send(self, data):
        line = json.dumps(data) + '\n'
        logging.info('send: %s', line.rstrip())
        with self.lock:
            self.conn.sendall(line.encode())
