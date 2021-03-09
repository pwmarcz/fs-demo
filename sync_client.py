"""
A client for sync_server.

Example usage:

    client = Client('server.sock')
    client.start()

    counter = SyncHandle(client, 'counter')

    # Reading:
    with counter.use(shared=True):
        print('counter value', counter.data)

    # Writing (with exclusive lock):
    with counter.use(shared=False):
        counter.data += 1
        counter.modified = True

    # Writing (without exclusive lock):
    with counter.use(shared=True):
        counter.data += 1
        counter.modified = True
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
    def use(self, shared: bool):
        """
        Lock a handle in either shared or exclusive mode.
        """

        with self.user_lock:
            self._acquire(shared)
            try:
                yield
            finally:
                self._release()

    def _acquire(self, shared: bool):
        with self.prop_lock:
            self.used = True
            # Need to reacquire if the handle is either invalid, or shared when
            # we want exclusive.
            if not self.valid or (not shared and self.shared):
                self.client.get(self, shared)
                return True
            return False

    def acquire(self, shared: bool) -> bool:
        """
        Lock a handle in shared/exclusive mode. You need to unlock it
        afterwards using release().

        Returns True if the handle had to be reacquired
        (i.e. it was invalid for a time.)
        """
        self.user_lock.acquire()
        try:
            return self._acquire(shared)
        except Exception:
            with self.prop_lock:
                self.used = False
            self.user_lock.release()
            raise

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
            'get': self.on_get,
            'req_put': self.on_req_put,
        }

    def get(self, handle: SyncHandle, shared: bool, update: bool = False):
        assert handle.user_lock.locked()
        assert handle.prop_lock.locked()
        if shared:
            assert not update

        with self.lock:
            self.handles[handle.key] = handle
        handle.valid = False
        self.send(['req_get', handle.key, shared, handle.data, update])
        # Wait until the handle is valid, and exclusive (if requested)
        while not (handle.valid and (shared or not handle.shared)):
            handle.ack.wait()

    def on_get(self, key, shared, data):
        with self.lock:
            handle = self.handles[key]

        with handle.prop_lock:
            handle.valid = True
            handle.modified = False
            handle.data = data
            handle.shared = shared
            handle.ack.notify_all()

    def on_req_put(self, key):
        with self.lock:
            handle = self.handles[key]

        with handle.prop_lock:
            if handle.used:
                # If handle is being used, send it back to the server after
                # we stop using it.
                handle.need_put = True
            else:
                self.put(handle)

    def put(self, handle):
        assert handle.prop_lock.locked()
        handle.valid = False
        handle.modified = False
        self.send(['put', handle.key, handle.data])
