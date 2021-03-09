"""
A server for synchronizing resources (sync handles). At a given time, a handle
can be either

* shared: loaded by possibly many clients,
* exclusive: loaded by a single client (and blocking other clients).

Client messages:

* ["req_get", key, shared, data, update]: The client requests resource,
  in either shared or exclusive mode. The server will respond by "get" when the
  resource can be used.

  If ``update`` is true, the data stored by handle will be updated. Otherwise,
  the value of ``data`` will be used only as a default. This can be used only
  in exclusive mode.

* ["put", key, data]: The client stops using resource, and reports latest
  version of data. The data will be updated only in exclusive mode.

Server messages:

* ["get", key, shared, data]: The client is permitted to use the resource, in
  either shared or exclusive mode.

* ["req_put", key]: The server requests client to to unload a resource. The
  client should respond with "put" when ready.
"""

import os
from dataclasses import dataclass
from threading import Lock
import logging
from typing import List, Any, Dict, Optional

from ipc import Conn, IpcServer


@dataclass
class Request:
    """
    A client request to use resource (req_get). Can be handled immediately, or
    queued.

    ``started`` means we already requested other clients to release the
    resource. This should happen for one request at a time.
    """

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

    def _handle_request(self, request: Request):
        """
        Handle a request, if possible. Returns True on success.
        """

        if request.shared and not self.exclusive_conn:
            assert request.conn not in self.shared_conns
            assert not request.update
            self.shared_conns.append(request.conn)
            request.conn.send(['get', self.key, True, self.data])
            return True

        if not request.shared and not self.exclusive_conn and \
                not self.shared_conns:
            self.exclusive_conn = request.conn
            if request.update:
                self.data = request.data
            request.conn.send(['get', self.key, False, self.data])
            return True

        return False

    def _start_request(self, request: Request):
        """
        Initiate handling a request (if immediate handling is not possible).
        This will request other clients to drop a resource.
        """

        if request.shared:
            assert self.exclusive_conn
            self.exclusive_conn.send(['req_put', self.key])
        else:
            if self.exclusive_conn:
                self.exclusive_conn.send(['req_put', self.key])
            for conn in self.shared_conns:
                conn.send(['req_put', self.key])

    def req_get(self, conn: Conn, shared: bool, data: Any, update: bool):
        if shared:
            assert not update

        if conn in self.shared_conns:
            self.shared_conns.remove(conn)
        if conn == self.exclusive_conn:
            self.exclusive_conn = None

        self.requests.append(Request(conn, shared, data, update, started=False))
        self._handle_requests()

    def put(self, conn: Conn, data: Any):
        if conn in self.shared_conns:
            self.shared_conns.remove(conn)
        elif conn == self.exclusive_conn:
            self.exclusive_conn = None
            self.data = data

        self.requests = [request for request in self.requests
                         if request.conn != conn]
        self._handle_requests()


class SyncServer(IpcServer):
    def __init__(self, path):
        super().__init__(path)

        self.methods = {
            'req_get': self.on_req_get,
            'put': self.on_put,
        }
        self.lock = Lock()
        self.handles: Dict[str, SyncServerHandle] = {}

    def _get_handle(self, key: str, default_data):
        assert self.lock.locked()
        if key not in self.handles:
            self.handles[key] = SyncServerHandle(key=key, data=default_data)
        return self.handles[key]

    def on_req_get(self, conn: Conn, key: str, shared: bool, data: Any,
                   update: bool):
        with self.lock:
            if key not in self.handles:
                self.handles[key] = SyncServerHandle(key, data)
            handle = self.handles[key]
            handle.req_get(conn, shared, data, update)

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
