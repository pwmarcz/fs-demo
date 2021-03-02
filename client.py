import abc
from contextlib import contextmanager
from threading import Lock
from typing import Type, ContextManager, Dict, Optional


class Object(abc.ABC):
    key: str
    local_lock: Lock
    update_lock: Lock

    def __init__(self, key: str):
        self.key = key
        self.local_lock = Lock()
        self.update_lock = Lock()

    @abc.abstractmethod
    def checkpoint(self) -> dict:
        pass

    @classmethod
    @abc.abstractclassmethod
    def restore_new(cls, key: str, data: dict) -> 'Object':
        pass

    @abc.abstractmethod
    def restore(self, data: dict) -> None:
        pass


class Client(abc.ABC):
    @abc.abstractmethod
    def register(self, obj: Object) -> None:
        pass

    @abc.abstractmethod
    def unregister(self, key: str) -> None:
        pass

    @abc.abstractmethod
    def get(self, key: str, cls: Type[Object]) -> Optional[Object]:
        pass

    @abc.abstractmethod
    def put(self, obj: Object) -> None:
        pass

    @abc.abstractmethod
    def update(self, obj: Object) -> ContextManager:
        pass

    @abc.abstractmethod
    def write(self, obj: Object) -> ContextManager:
        pass


class LocalClient(Client):
    def __init__(self):
        self.store: Dict[str, Object] = {}

    def register(self, obj):
        self.store[obj.key] = obj
        obj.local_lock.acquire()

    def unregister(self, key):
        obj = self.store[key]
        with obj.local_lock:
            del self.store[key]

    def get(self, key, cls):
        if key not in self.store:
            return None
        obj = self.store[key]
        obj.local_lock.acquire()
        return obj

    def put(self, obj):
        obj.local_lock.release()

    @contextmanager
    def update(self, obj):
        with obj.update_lock:
            yield

    @contextmanager
    def write(self, obj):
        yield
