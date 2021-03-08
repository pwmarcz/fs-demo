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
from enum import Enum
from threading import Lock
from contextlib import contextmanager
from threading import Condition
from typing import Dict

from ipc import IpcClient


class SyncHandle:
    """
    A handle for a resource. Can be used as a container for data, or just as a
    lock.

    The data can be used only when the user_lock is held. Otherwise, the handle
    can be unloaded at any time by the client.
    """

    def __init__(self, client: 'SyncClient', key: str, data=None):
        self.key = key
        self.client = client

        self.user_lock = Lock()
        self.prop_lock = Lock()
        self.ack = Condition(self.prop_lock)

        # Protected by user_lock
        self.data = data
        self.modified = False

        # Protected by prop_lock
        self.used = False
        self.valid = False
        self.shared = False
        self.need_put = False

    @contextmanager
    def get_shared(self):
        """
        Lock a handle in shared mode. This can be done by many clients, but
        guarantees no other client will modify the handle until lock is held.
        """

        with self.user_lock:
            self._acquire(shared=True)
            yield
            self._release()

    @contextmanager
    def get_exclusive(self):
        """
        Lock a handle in exclusive mode. This will guarantee no other client
        will use it the same time (in either shared or exclusive mode).
        """

        with self.user_lock:
            if not self.valid:
                self.client.get(self, shared=False)
            yield

    def _acquire(self, shared=True):
        with self.prop_lock:
            self.used = True
            if not self.valid or (not shared and self.shared):
                self.client.get(self, shared)
                return True
            return False

    def acquire(self, shared=True) -> bool:
        """
        Lock a handle in shared/exclusive mode. You need to unlock it
        afterwards using release().

        Returns True if the handle has changed (i.e. it was invalid for a time.)
        """
        self.user_lock.acquire()
        return self._acquire(shared)

    def _release(self):
        with self.prop_lock:
            if self.modified and self.shared:
                self.client.get(self, shared=False, update=True)
            self.used = False
            if self.need_put:
                self.client.put(self)
                self.need_put = False

    def release(self):
        assert self.user_lock.locked()
        self._release()
        self.user_lock.release()


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

    def get(self, handle: SyncHandle, shared: bool, update: bool = False):
        assert handle.user_lock.locked()
        assert handle.prop_lock.locked()
        with self.lock:
            self.handles[handle.key] = handle
        handle.valid = False
        if shared:
            self.send(['get', handle.key, handle.data])
        else:
            self.send(['get_exclusive', handle.key, handle.data, update])
        handle.ack.wait()

    def on_ack(self, key, data, shared):
        with self.lock:
            handle = self.handles[key]

        with handle.prop_lock:
            handle.valid = True
            handle.modified = False
            handle.data = data
            handle.shared = shared
            handle.ack.notify()

    def on_drop(self, key):
        with self.lock:
            handle = self.handles[key]

        with handle.prop_lock:
            if handle.used:
                handle.need_put = True
            else:
                self.put(handle)

    def put(self, handle):
        assert handle.prop_lock.locked()
        handle.valid = False
        handle.modified = False
        self.send(['put', handle.key, handle.data])
