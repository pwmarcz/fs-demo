"""
A server for synchronizing resources (sync handles). At a given time, a client
can hold the handle in one of three states:

* State.INVALID: unloaded,
* State.SHARED: loaded in shared mode (possibly together with other clients),
* State.EXLUSIVE: loaded in exclusive mode (and blocking other clients).

Client messages:

* ["req_up", key, state, data, update]: The client requests to upgrade a
  resource (to either shared or exclusive mode). The server will respond by
  "up" when the resource can be used.

  If ``update`` is true, the data stored by handle will be updated. Otherwise,
  the value of ``data`` will be used only as a default. This can be used only
  in exclusive mode.

* ["down", key, state, data]: The client downgrades resource (to shared/invalid
  mode), and reports latest version of data. The data will be updated only when
  in exclusive mode.

Server messages:

* ["up", key, state, data]: The client is permitted to use the resource, in
  either shared or exclusive mode.

* ["req_down", key, state]: The server requests client to to downgrade a
  resource. The client should respond with "down" when ready.
"""

import os
from dataclasses import dataclass
from enum import Enum, IntEnum
from threading import Lock
import logging
from typing import List, Any, Dict, Optional

from ipc import Conn, IpcServer


class State(IntEnum):
    INVALID = 0
    SHARED = 1
    EXCLUSIVE = 2


@dataclass
class Request:
    """
    A client request to use resource (req_get). Can be handled immediately, or
    queued.

    ``started`` means we already requested other clients to release the
    resource. This should happen for one request at a time.
    """

    conn: Conn
    want_state: State
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

    def unused(self):
        return not (self.shared_conns or self.exclusive_conn or self.requests)

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

        if request.want_state == State.SHARED and not self.exclusive_conn:
            assert request.conn not in self.shared_conns
            assert not request.update
            self.shared_conns.append(request.conn)
            request.conn.send(['up', self.key, State.SHARED.name, self.data])
            return True

        if (request.want_state == State.EXCLUSIVE and
                not self.exclusive_conn and
                not self.shared_conns):
            self.exclusive_conn = request.conn
            if request.update:
                self.data = request.data
            request.conn.send(['up', self.key, State.EXCLUSIVE.name, self.data])
            return True

        return False

    def _start_request(self, request: Request):
        """
        Initiate handling a request (if immediate handling is not possible).
        This will request other clients to drop a resource.
        """

        if request.want_state == State.SHARED:
            assert self.exclusive_conn
            self.exclusive_conn.send(['req_down', self.key, State.SHARED.name])
        else:
            if self.exclusive_conn:
                self.exclusive_conn.send(['req_down', self.key, State.INVALID.name])
            for conn in self.shared_conns:
                conn.send(['req_down', self.key, State.INVALID.name])

    def req_up(self, conn: Conn, state: State, data: Any, update: bool):
        assert state in [State.SHARED, State.EXCLUSIVE]

        if state == State.SHARED:
            assert not update

        if conn in self.shared_conns:
            self.shared_conns.remove(conn)
        if conn == self.exclusive_conn:
            self.exclusive_conn = None

        self.requests.append(Request(conn, state, data, update, started=False))
        self._handle_requests()

    def down(self, conn: Conn, state: State, data: Any):
        assert state in [State.INVALID, State.SHARED]

        if state == State.INVALID:
            if conn in self.shared_conns:
                self.shared_conns.remove(conn)
            elif conn == self.exclusive_conn:
                self.exclusive_conn = None
                self.data = data
        else:
            if conn == self.exclusive_conn:
                self.exclusive_conn = None
                self.data = data
                self.shared_conns.append(conn)

        self.requests = [request for request in self.requests
                         if request.conn != conn]
        self._handle_requests()


class SyncServer(IpcServer):
    def __init__(self, path):
        super().__init__(path)

        self.methods = {
            'req_up': self.on_req_up,
            'down': self.on_down,
        }
        self.lock = Lock()
        self.handles: Dict[str, SyncServerHandle] = {}

    def _get_handle(self, key: str, default_data):
        assert self.lock.locked()
        if key not in self.handles:
            logging.info('server: creating new handle: %r, %r', key, default_data)
            self.handles[key] = SyncServerHandle(key=key, data=default_data)
        return self.handles[key]

    def on_req_up(self, conn: Conn, key: str, state_name: str, data: Any,
                   update: bool):
        with self.lock:
            if key not in self.handles:
                self.handles[key] = SyncServerHandle(key, data)
            handle = self.handles[key]
            handle.req_up(conn, State[state_name], data, update)

    def on_down(self, conn: Conn, key: str, state_name: str, data: Any):
        with self.lock:
            handle = self.handles[key]
            handle.down(conn, State[state_name], data)

    def remove_conn(self, conn: Conn):
        with self.lock:
            for key, handle in list(self.handles.items()):
                handle.down(conn, State.INVALID, handle.data)
                if handle.unused():
                    logging.info('server: deleting unused handle: %r, %r',
                                 handle.key, handle.data)
                    del self.handles[key]
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
