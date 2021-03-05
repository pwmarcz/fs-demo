import os
from dataclasses import dataclass
from threading import Lock
import logging
from typing import List, Any, Dict

from ipc import Conn, IpcServer


@dataclass
class SyncServerHandle:
    key: str
    data: Any
    conns: List[Conn]


class SyncServer(IpcServer):
    def __init__(self, path):
        super().__init__(path)

        self.methods = {
            'load': self.on_load,
            'unload': self.on_unload,
            'modify': self.on_modify,
        }
        self.lock = Lock()
        self.handles: Dict[str, SyncServerHandle] = {}

    def handle_message(self, conn: Conn, data):
        method, args = data[0], data[1:]
        if method not in self.methods:
            raise KeyError(f'unknown method: {method}')
        self.methods[method](conn, *args)

    def on_load(self, conn: Conn, key: str, default_data=None):
        with self.lock:
            if key not in self.handles:
                logging.info('load %r (create)', key)
                handle = SyncServerHandle(key=key, data=default_data, conns=[conn])
                self.handles[key] = handle
            else:
                handle = self.handles[key]
                assert conn not in handle.conns
                if len(handle.conns) == 1:
                    logging.info('load %r (share)', key)
                    handle.conns[0].send(['share', key])
                else:
                    logging.info('load %r', key)
                handle.conns.append(conn)
            shared = len(handle.conns) > 1
            conn.send(['ack', key, handle.data, shared])

    def on_unload(self, conn: Conn, key: str):
        with self.lock:
            handle = self.handles[key]
            self._unload(handle, conn)
            conn.send(['ack', key, None, False])

    def _unload(self, handle: SyncServerHandle, conn: Conn):
        assert self.lock.locked()
        assert conn in handle.conns
        handle.conns.remove(conn)
        if len(handle.conns) == 1:
            logging.info('unload %r (unshare)', handle.key)
            handle.conns[0].send(['unshare', handle.key])
        elif len(handle.conns) == 0:
            logging.info('unload %r (delete)', handle.key)
            del self.handles[handle.key]
        else:
            logging.info('unload %r', handle.key)

    def on_modify(self, conn: Conn, key: str, new_data=None):
        with self.lock:
            handle = self.handles[key]
            handle.data = new_data
            assert conn in handle.conns
            assert len(handle.conns) > 1
            logging.info('modify %r', key)
            for other in handle.conns:
                if other != conn:
                    other.send(['unload', key])
            handle.conns = [conn]
            shared = False
            conn.send(['ack', key, handle.data, shared])

    def remove_conn(self, conn: Conn):
        with self.lock:
            for key, handle in list(self.handles.items()):
                if conn in handle.conns:
                    self._unload(handle, conn)
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
