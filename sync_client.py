"""
A client for sync_server.

Example usage:

    client = Client('server.sock')
    client.start()

    counter = SyncHandle(client, 'counter')

    # Reading:
    with counter.get_shared():
        print('counter value', counter.data)

    # Writing (with exclusive lock):
    with counter.get_exclusive():
        counter.data += 1
        counter.modified = True

    # Writing (without exclusive lock):
    with counter.get_shared():
        counter.data += 1
        counter.modified = 1
"""

from threading import Lock
from contextlib import contextmanager
from threading import Condition
from typing import Dict

from ipc import IpcClient


class SyncHandle:
    """
    A handle for a resource. Can be used as a container for data, or just as a
    lock.

    The data can be used only when the lock is held (i.e. inside get_shared()
    and get_exclusive()). Otherwise, the handle can be unloaded at any time by
    the client.
    """

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
        """
        Lock a handle in shared mode. This can be done by many clients, but
        guarantees no other client will modify the handle until lock is held.
        """

        with self.lock:
            if not self.valid:
                self.client.get_shared(self)
            yield
            if self.modified and self.shared:
                self.client.get_exclusive(self, update=True)

    @contextmanager
    def get_exclusive(self):
        """
        Lock a handle in exclusive mode. This will guarantee no other client
        will use it the same time (in either shared or exclusive mode).
        """

        with self.lock:
            if not self.valid:
                self.client.get_exclusive(self)
            yield

    def acquire_shared(self):
        self.lock.acquire()
        if not self.valid:
            self.client.get_shared(self)
            return False
        return True

    def acquire_exclusive(self):
        self.lock.acquire()
        if not self.valid:
            self.client.get_exclusive(self)
            return False
        return True

    def release(self):
        assert self.lock.locked()
        if self.modified and self.shared:
            self.client.get_exclusive(self, update=True)
        self.lock.release()


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
