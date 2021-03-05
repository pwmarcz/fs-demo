from threading import Lock
from contextlib import contextmanager
from threading import Condition
from typing import Dict
import logging

from ipc import IpcClient


class SyncHandle:
    def __init__(self, client: 'SyncClient', key: str, data=None):
        self.lock = Lock()
        self.ack = Condition(self.lock)
        self.key = key
        self.data = data
        self.valid = False
        self.shared = False
        self.modified = False
        self.client = client

    @contextmanager
    def get(self):
        with self.lock:
            if not self.valid:
                self._load()
            yield
            if self.modified:
                self._modify()
                self.modified = False

    def _load(self):
        assert self.lock.locked()
        assert not self.valid
        self.client.load(self)
        self.ack.wait()
        self.valid = True

    def _modify(self):
        assert self.lock.locked()
        assert self.valid
        if self.shared:
            self.client.modify(self)
            self.ack.wait()


class SyncClient(IpcClient):
    def __init__(self, path):
        super(SyncClient, self).__init__(path)
        self.lock = Lock()
        self.handles: Dict[str, SyncHandle] = {}

        self.methods = {
            'ack': self.on_ack,
            'share': self.on_share,
            'unshare': self.on_unshare,
            'unload': self.on_unload,
        }

    def handle_message(self, data):
        method, args = data[0], data[1:]
        if method not in self.methods:
            raise KeyError(f'unknown method: {method}')
        self.methods[method](*args)

    def load(self, handle: SyncHandle):
        with self.lock:
            self.handles[handle.key] = handle
        self.send(['load', handle.key, handle.data])

    def modify(self, handle: SyncHandle):
        self.send(['modify', handle.key, handle.data])

    def on_ack(self, key, data, shared):
        with self.lock:
            handle = self.handles[key]

        with handle.lock:
            handle.data = data
            handle.shared = shared
            handle.ack.notify()

    def on_share(self, key):
        with self.lock:
            handle = self.handles[key]

        with handle.lock:
            handle.shared = True

    def on_unshare(self, key):
        with self.lock:
            handle = self.handles[key]

        with handle.lock:
            handle.shared = False

    def on_unload(self, key):
        with self.lock:
            handle = self.handles[key]

        with handle.lock:
            handle.valid = False


logging.basicConfig(level=logging.INFO, format='[%(threadName)s] %(message)s')
client = SyncClient('server.sock')
client.start()

handle = SyncHandle(client, 'a', 123)
with handle.get():
    pass
