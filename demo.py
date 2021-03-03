import abc
import logging
import os
import random
import tempfile
import time
from contextlib import contextmanager
from functools import wraps
from threading import Lock, Thread
from typing import ContextManager, Dict, List, Optional

# from client import Client, LocalClient, Object


def random_sleep():
    time.sleep(random.randrange(0, 3) * 0.05)


class RefCount:
    def __init__(self, value=1):
        self.lock = Lock()
        self.value = value

    def inc(self):
        with self.lock:
            assert self.value > 0
            self.value += 1

    def dec(self):
        with self.lock:
            assert self.value > 0
            self.value -= 1
            return self.value == 0


class Inode:
    def __init__(
        self,
        ident: str,
        is_dir: bool,
        size: int,
        data: dict,
        mount: 'Mount',
    ):
        self.ident = ident
        self.is_dir = is_dir
        self.size = size
        self.data = data
        self.mount = mount
        self.mount_here: Optional['Mount'] = None

        self.ref_count = RefCount()
        self.size_lock = Lock()


class Mount(metaclass=abc.ABCMeta):
    ident: str
    root_inode: Inode

    @abc.abstractmethod
    def lookup(self, inode: Inode, name: str) -> Optional[str]:
        pass

    @abc.abstractmethod
    def readdir(self, inode: Inode) -> List[str]:
        pass

    @abc.abstractmethod
    def load(self, ident: str) -> Inode:
        pass

    @abc.abstractmethod
    def create_file(self, inode: Inode, name: str) -> str:
        pass

    @abc.abstractmethod
    def create_dir(self, inode: Inode, name: str) -> str:
        pass

    @abc.abstractmethod
    def read(self, inode: Inode, offset: int, length: int) -> bytes:
        pass

    @abc.abstractmethod
    def write(self, inode: Inode, offset: int, data: bytes) -> int:
        pass


class HostMount(Mount):
    def __init__(self, ident: str, root_path: str):
        self.ident = ident
        self.root_path = root_path
        self.root_inode = self.load(root_path)

    def lookup(self, inode: Inode, name: str) -> Optional[str]:
        host_path = inode.data['host_path'] + '/' + name
        if os.path.exists(host_path):
            return host_path

        return None

    def readdir(self, inode: Inode) -> List[str]:
        return os.listdir(inode.data['host_path'])

    def load(self, ident: str) -> Inode:
        host_path = ident
        if os.path.isdir(host_path):
            size = 0
            is_dir = True
        else:
            size = os.stat(host_path).st_size
            is_dir = False
        inode = Inode(
            ident=ident,
            size=size,
            is_dir=is_dir,
            data={'host_path': host_path},
            mount=self
        )
        return inode

    def create_file(self, inode: Inode, name: str) -> str:
        host_path = inode.data['host_path'] + '/' + name
        with open(host_path, 'wb') as f:
            pass
        return host_path

    def create_dir(self, inode: Inode, name: str) -> str:
        host_path = inode.data['host_path'] + '/' + name
        os.mkdir(host_path)
        return host_path

    def read(self, inode: Inode, offset: int, length: int) -> bytes:
        with open(inode.data['host_path'], 'rb') as f:
            f.seek(offset)
            return f.read(length)

    def write(self, inode: Inode, offset: int, data: bytes) -> int:
        with open(inode.data['host_path'], 'r+b') as f:
            f.seek(offset)
            result = f.write(data)
        return result


class MemMount(Mount):
    def __init__(self, ident: str):
        self.ident = ident
        self.counter = 1
        self.files: dict = {
            '0': {}
        }
        self.lock = Lock()
        self.root_inode = self.load('0')

    def lookup(self, inode: Inode, name: str) -> Optional[str]:
        with self.lock:
            f = self.files[inode.ident]
            assert isinstance(f, dict)
            return f.get(name)

    def readdir(self, inode: Inode):
        with self.lock:
            f = self.files[inode.ident]
            assert isinstance(f, dict)
            return list(f.keys())

    def load(self, ident: str) -> Inode:
        with self.lock:
            f = self.files[ident]
            if isinstance(f, dict):
                size = 0
                is_dir = True
            else:
                size = len(f)
                is_dir = False
            return Inode(
                ident=ident,
                size=size,
                is_dir=is_dir,
                data={},
                mount=self
            )

    def _create(self, inode: Inode, name: str, initial) -> str:
        with self.lock:
            f = self.files[inode.ident]
            assert isinstance(f, dict)
            assert name not in f
            ident = str(self.counter)
            self.counter += 1
            f[name] = ident
            self.files[ident] = initial
            return ident

    def create_file(self, inode: Inode, name: str) -> str:
        return self._create(inode, name, bytearray())

    def create_dir(self, inode: Inode, name: str):
        return self._create(inode, name, {})

    def read(self, inode: Inode, pos: int, length: int) -> bytes:
        with self.lock:
            f = self.files[inode.ident]
            assert isinstance(f, bytearray)
            return bytes(f[pos:pos+length])

    def write(self, inode: Inode, pos: int, data: bytes) -> int:
        with self.lock:
            f = self.files[inode.ident]
            assert isinstance(f, bytearray)
            if len(f) < pos:
                f.extend(b'\x00' * (pos - len(f)))
            f[pos:pos+len(data)] = data
            return len(data)


class Handle:
    def __init__(
        self,
        inode: Inode,
        append: bool
    ):
        self.inode = inode
        self.append = append
        self.pos = 0
        self.ref_count = RefCount()
        self.pos_lock = Lock()


def trace(method):
    @wraps(method)
    def wrapped(self, *args, **kwargs):
        name = method.__name__
        args_str = ', '.join(repr(arg) for arg in args)
        if kwargs:
            args_str += ', '
            args_str += ', '.join(f'{k}={v!r}' for k, v in kwargs.items())
        logging.info(f'--- {name}({args_str})')
        result = method(self, *args, **kwargs)
        logging.info(f'--- {name}({args_str}) = {result!r}')
        return result

    return wrapped


class InodeCache:
    def __init__(self):
        self.inodes: Dict[str, Inode] = {}
        self.lock = Lock()

    def add(self, inode):
        key = (inode.mount.ident, inode.ident)
        with self.lock:
            assert key not in self.inodes
            self.inodes[key] = inode

    def inc(self, inode):
        inode.ref_count.inc()

    def dec(self, inode):
        with self.lock:
            if inode.ref_count.dec():
                key = (inode.mount.ident, inode.ident)
                del self.inodes[key]

    def get(self, mount, ident):
        with self.lock:
            return self.inodes.get((mount.ident, ident))


class FS:
    def __init__(self, root_mount: Mount):
        self.root_mount = root_mount
        self.root_inode = root_mount.root_inode

        self.inode_cache = InodeCache()
        self.handles: Dict[int, Handle] = {}
        self.global_lock = Lock()

        self.inode_cache.add(root_mount.root_inode)
        self.mounts = {root_mount.ident: root_mount}

    def find_inode(self, mount: Mount, ident: str):
        logging.info(f'find_inode: {ident}')
        with self.global_lock:
            inode = self.inode_cache.get(mount, ident)
            if inode:
                self.inode_cache.inc(inode)
            else:
                inode = mount.load(ident)
                self.inode_cache.add(inode)
        return inode

    def create_handle(self, inode, append):
        logging.info(f'create_handle: {inode.ident}')
        self.inode_cache.inc(inode)
        handle = Handle(
            inode=inode,
            append=append
        )
        with self.global_lock:
            fd = 0
            while fd in self.handles:
                fd += 1
            self.handles[fd] = handle
        return handle, fd

    def put_handle(self, handle):
        logging.info(f'put_handle: {handle.inode.ident}')
        if handle.ref_count.dec():
            logging.info(f'delete_handle: {handle.inode.ident}')
            self.inode_cache.dec(handle.inode)

    def _traverse_mount(self, inode: Inode):
        if inode.mount_here:
            self.inode_cache.dec(inode)
            inode = inode.mount_here.root_inode
            self.inode_cache.inc(inode)
        return inode

    @contextmanager
    def find(self, path: str, create: bool = False) -> ContextManager[Inode]:
        if path == '/':
            names = []
        else:
            names = path.lstrip('/').split('/')

        inode = self.root_inode
        self.inode_cache.inc(inode)
        inode = self._traverse_mount(inode)
        for i, name in enumerate(names):
            try:
                next_ident = inode.mount.lookup(inode, name)
                if next_ident is None:
                    if create and i == len(names) - 1:
                        next_ident = inode.mount.create_file(inode, name)
                    else:
                        raise Exception(f'Lookup {name!r} failed')
            finally:
                self.inode_cache.dec(inode)

            inode = self.find_inode(inode.mount, next_ident)
            inode = self._traverse_mount(inode)

        try:
            yield inode
        finally:
            self.inode_cache.dec(inode)

    @trace
    def mount(self, path: str, mount: Mount) -> None:
        with self.find(path) as inode:
            with self.global_lock:
                self.mounts[mount.ident] = mount
                inode.mount_here = mount
                self.inode_cache.inc(inode)
                self.inode_cache.add(mount.root_inode)

    @trace
    def readdir(self, path: str) -> List[str]:
        with self.find(path) as inode:
            return inode.mount.readdir(inode)

    @trace
    def stat(self, path: str) -> dict:
        with self.find(path) as inode:
            if inode.is_dir:
                return {'type': 'dir'}
            else:
                with inode.size_lock:
                    return {'type': 'file', 'size': inode.size}

    @trace
    def open(self, path: str, create=False, append=False) -> int:
        with self.find(path, create=create) as inode:
            assert not inode.is_dir
            handle, fd = self.create_handle(inode, append)
            return fd

    @trace
    def read(self, fd, length):
        handle = self.handles[fd]
        inode = handle.inode
        with handle.pos_lock:
            random_sleep()
            result = inode.mount.read(inode, handle.pos, length)
            handle.pos += len(result)
        return result

    @trace
    def seek(self, fd, pos):
        handle = self.handles[fd]
        with handle.pos_lock:
            handle.pos = pos

    @trace
    def tell(self, fd):
        handle = self.handles[fd]
        with handle.pos_lock:
            return handle.pos

    @trace
    def write(self, fd, data: bytes) -> int:
        handle = self.handles[fd]
        inode = handle.inode
        if handle.append:
            with inode.size_lock:
                random_sleep()
                result = inode.mount.write(inode, inode.size, data)
                inode.size += result
        else:
            with handle.pos_lock:
                random_sleep()
                result = inode.mount.write(inode, handle.pos, data)
                handle.pos += result
                with inode.size_lock:
                    inode.size = max(inode.size, handle.pos)
        return result

    @trace
    def close(self, fd):
        with self.global_lock:
            handle = self.handles[fd]
            del self.handles[fd]
        self.put_handle(handle)

    @trace
    def mkdir(self, path):
        parent, _, name = path.rpartition('/')
        if parent == '':
            parent = '/'
        with self.find(parent) as inode:
            inode.mount.create_dir(inode, name)


def writer(fs):
    fd = fs.open('/log.txt', create=True, append=True)
    for i in range(5):
        random_sleep()
        fs.write(fd, 'log line {} (different fds)\n'.format(i).encode())
    fs.close(fd)


def writer_fd(fs, fd):
    for i in range(5):
        random_sleep()
        fs.write(fd, 'log line {} (same fd)\n'.format(i).encode())


def repeat(target, args, n):
    threads = [Thread(target=target, args=args) for i in range(n)]
    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()


def main():
    logging.basicConfig(level=logging.INFO, format='[%(threadName)s] %(message)s')

    with tempfile.TemporaryDirectory() as d:
        mount = MemMount('mem')
        fs = FS(mount)

        fs.mkdir('/host')

        mount2 = HostMount('host', d)
        fs.mount('/host', mount2)

        repeat(writer, [fs], 2)

        fd = fs.open('/log.txt')
        fs.seek(fd, fs.stat('/log.txt')['size'])
        repeat(writer_fd, [fs, fd], 2)

        fd = fs.open('/log.txt')
        result = fs.read(fd, 4096)
        print(result.decode())


if __name__ == '__main__':
    main()
