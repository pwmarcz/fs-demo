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
from enum import Enum
from threading import Lock
from contextlib import contextmanager
from threading import Condition
from typing import Dict, Optional

from ipc import IpcClient
from sync_server import State


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
        self.state: State = State.INVALID
        self.next_state: Optional[State] = None

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
        want_state = State.SHARED if shared else State.EXCLUSIVE
        with self.prop_lock:
            self.used = True
            # Need to reacquire if the handle is either invalid, or shared when
            # we want exclusive.
            if self.state < want_state:
                self.client.up(self, want_state)
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
            if self.modified and self.state == State.SHARED:
                self.client.up(self, State.EXCLUSIVE, update=True)
            self.used = False
            if self.next_state is not None:
                self.client.down(self, self.next_state)
                self.next_state = None

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
            'up': self.on_up,
            'req_down': self.on_req_down,
        }

    def up(self, handle: SyncHandle, want_state: State, update: bool = False):
        assert handle.user_lock.locked()
        assert handle.prop_lock.locked()
        if want_state == State.SHARED:
            assert not update

        with self.lock:
            self.handles[handle.key] = handle
        handle.state = State.INVALID
        self.send(['req_up', handle.key, want_state.value, handle.data, update])
        # Wait until the handle is valid, and exclusive (if requested)
        while handle.state < want_state:
            handle.ack.wait()

    def on_up(self, key, state_val: int, data):
        with self.lock:
            handle = self.handles[key]

        with handle.prop_lock:
            handle.modified = False
            handle.data = data
            handle.state = State(state_val)
            handle.ack.notify_all()

    def on_req_down(self, key, state_val: int):
        with self.lock:
            handle = self.handles[key]

        state = State(state_val)

        with handle.prop_lock:
            if handle.used:
                # If handle is being used, send it back to the server after
                # we stop using it.
                handle.next_state = state
            else:
                self.down(handle, state)

    def down(self, handle: SyncHandle, state: State):
        assert handle.prop_lock.locked()
        handle.state = state
        handle.modified = False
        self.send(['down', handle.key, handle.state, handle.data])
