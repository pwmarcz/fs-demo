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
    def get_shared(self):
        with self.lock:
            if not self.valid:
                self.client.get_shared(self)
            yield
            if self.modified:
                if self.shared:
                    self.client.get_exclusive(self, update=True)

    @contextmanager
    def get_exclusive(self):
        with self.lock:
            if not self.valid:
                self.client.get_exclusive(self)
            yield


class SyncClient(IpcClient):
    def __init__(self, path):
        super(SyncClient, self).__init__(path)
        self.lock = Lock()
        self.handles: Dict[str, SyncHandle] = {}

        self.methods = {
            'ack': self.on_ack,
            'drop': self.on_drop,
        }

    def handle_message(self, data):
        method, args = data[0], data[1:]
        if method not in self.methods:
            raise KeyError(f'unknown method: {method}')
        self.methods[method](*args)

    def get_shared(self, handle: SyncHandle):
        assert handle.lock.locked()
        with self.lock:
            self.handles[handle.key] = handle
        handle.valid = False
        self.send(['get', handle.key, handle.data])
        handle.ack.wait()

    def get_exclusive(self, handle: SyncHandle, update=False):
        assert handle.lock.locked()
        with self.lock:
            self.handles[handle.key] = handle
        handle.valid = False
        self.send(['get_exclusive', handle.key, handle.data, update])
        handle.ack.wait()

    def on_ack(self, key, data, shared):
        with self.lock:
            handle = self.handles[key]

        with handle.lock:
            handle.valid = True
            handle.modified = False
            handle.data = data
            handle.shared = shared
            handle.ack.notify()

    def on_drop(self, key):
        with self.lock:
            handle = self.handles[key]

        with handle.lock:
            handle.valid = False
            handle.modified = False
            self.send(['put', handle.key, handle.data])

'''
logging.basicConfig(level=logging.INFO, format='[%(threadName)s] %(message)s')
client = SyncClient('server.sock')
client.start()

import time
handle = SyncHandle(client, 'a', 0)

while True:
    with handle.get_exclusive():
        handle.data += 1
        handle.modified = True
        print(handle.data)
        time.sleep(0.5)
    time.sleep(0.5)
'''
