import os
from dataclasses import dataclass
from threading import Lock
import logging
from typing import List, Any, Dict, Optional, Tuple

from ipc import Conn, IpcServer


@dataclass
class Request:
    conn: Conn
    shared: bool
    data: Any
    update: bool
    started: bool


class SyncServerHandle:
    shared_conns: List[Conn]
    exclusive_conn: Optional[Conn]
    requests: List[Request]

    def __init__(self, key: str, data: Any):
        self.key = key
        self.data = data
        self.shared_conns = []
        self.exclusive_conn = None
        self.requests = []

    def _handle_requests(self):
        while self.requests:
            request = self.requests[0]
            if self._handle_request(request):
                self.requests.pop(0)
                continue

            if not request.started:
                self._start_request(request)
                request.started = True
            break

    def _start_request(self, request: Request):
        if request.shared:
            assert self.exclusive_conn
            self.exclusive_conn.send(['drop', self.key])
        else:
            if self.exclusive_conn:
                self.exclusive_conn.send(['drop', self.key])
            else:
                assert self.shared_conns
                for conn in self.shared_conns:
                    conn.send(['drop', self.key])

    def _handle_request(self, request: Request):
        if request.shared and not self.exclusive_conn:
            assert request.conn not in self.shared_conns
            self.shared_conns.append(request.conn)
            request.conn.send(['ack', self.key, self.data, True])
            return True

        if not request.shared and not self.exclusive_conn and not self.shared_conns:
            self.exclusive_conn = request.conn
            if request.update:
                self.data = request.data
            request.conn.send(['ack', self.key, self.data, False])
            return True

        return False

    def get_shared(self, conn: Conn):
        if conn in self.shared_conns:
            return
        if conn == self.exclusive_conn:
            self.exclusive_conn = None

        self.requests.append(Request(conn=conn, shared=True, data=None, update=False, started=False))
        self._handle_requests()

    def get_exclusive(self, conn: Conn, data: Any, update: bool):
        if conn in self.shared_conns:
            self.shared_conns.remove(conn)
        if conn == self.exclusive_conn:
            return

        self.requests.append(Request(conn=conn, shared=False, data=data, update=update, started=False))
        self._handle_requests()

    def put(self, conn: Conn, data: Any):
        if conn in self.shared_conns:
            self.shared_conns.remove(conn)
        elif conn == self.exclusive_conn:
            self.exclusive_conn = None
            self.data = data

        self.requests = [request for request in self.requests if request.conn != conn]
        self._handle_requests()


class SyncServer(IpcServer):
    def __init__(self, path):
        super().__init__(path)

        self.methods = {
            'get': self.on_get,
            'get_exclusive': self.on_get_exclusive,
            'put': self.on_put,
        }
        self.lock = Lock()
        self.handles: Dict[str, SyncServerHandle] = {}

    def _get_handle(self, key: str, default_data=None):
        assert self.lock.locked()
        if key not in self.handles:
            self.handles[key] = SyncServerHandle(key=key, data=default_data)
        return self.handles[key]

    def on_get(self, conn: Conn, key: str, default_data=None):
        with self.lock:
            handle = self._get_handle(key, default_data)
            handle.get_shared(conn)

    def on_get_exclusive(self, conn: Conn, key: str, data=None, update=False):
        with self.lock:
            handle = self._get_handle(key, data)
            handle.get_exclusive(conn, data, update)

    def on_put(self, conn: Conn, key: str, data: Any):
        with self.lock:
            handle = self.handles[key]
            handle.put(conn, data)

    def remove_conn(self, conn: Conn):
        with self.lock:
            for key, handle in self.handles.items():
                handle.put(conn, handle.data)
        super().remove_conn(conn)

def main():
    logging.basicConfig(level=logging.INFO, format='[%(threadName)s] %(message)s')

    path = 'server.sock'
    if os.path.exists(path):
        os.unlink(path)
    server = SyncServer(path)
    server.run()


if __name__ == '__main__':
    main()
