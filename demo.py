import os
from typing import List, Dict, ContextManager
import tempfile
import logging
from contextlib import contextmanager
from dataclasses import dataclass
from functools import wraps
import time
from threading import Lock, Thread
import random

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


@dataclass
class Inode:
    host_path: str
    is_dir: bool
    entries: Dict[str, str]
    size: int

    ref_count: RefCount
    size_lock: Lock


@dataclass
class Handle:
    inode: Inode
    pos: int
    append: bool

    ref_count: RefCount
    pos_lock: Lock


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


class FS:
    def __init__(self, root_path: str):
        self.root_path = root_path

        self.inodes: Dict[str, str] = {}
        self.handles: Dict[int, Handle] = {}
        self.global_lock = Lock()

    def find_inode(self, host_path: str):
        logging.info(f'find_inode: {host_path}')
        with self.global_lock:
            if host_path in self.inodes:
                inode = self.inodes[host_path]
                self.get_inode(inode)
            else:
                inode = self.create_inode(host_path)
        return inode

    def get_inode(self, inode: Inode):
        logging.info(f'get_inode: {inode.host_path}')
        inode.ref_count.inc()

    def put_inode(self, inode: Inode):
        logging.info(f'put_inode: {inode.host_path}')
        with self.global_lock:
            if inode.ref_count.dec():
                logging.info(f'delete_inode: {inode.host_path}')
                del self.inodes[inode.host_path]

    def create_inode(self, host_path):
        logging.info(f'create_inode: {host_path}')
        if os.path.isdir(host_path):
            entries = {
                name: host_path + '/' + name
                for name in os.listdir(host_path)
            }
            size = 0
            is_dir = True
        else:
            entries = {}
            size = os.stat(host_path).st_size
            is_dir = False
        inode = Inode(
            host_path=host_path, entries=entries, size=size,
            is_dir=is_dir, ref_count=RefCount(), size_lock=Lock())
        self.inodes[host_path] = inode
        return inode

    def create_handle(self, inode, append):
        logging.info(f'create_handle: {inode.host_path}')
        self.get_inode(inode)
        handle = Handle(
            inode=inode, append=append, pos=0, ref_count=RefCount(),
            pos_lock=Lock())
        with self.global_lock:
            fd = 0
            while fd in self.handles:
                fd += 1
            self.handles[fd] = handle
        return handle, fd

    def put_handle(self, handle):
        logging.info(f'put_handle: {handle.inode.host_path}')
        if handle.ref_count.dec():
            logging.info(f'delete_handle: {handle.inode.host_path}')
            self.put_inode(handle.inode)

    @contextmanager
    def lookup(self, path: str, create: bool = False) -> ContextManager[Inode]:
        if path == '/':
            names = []
        else:
            names = path.lstrip('/').split('/')

        host_path = self.root_path
        inode = self.find_inode(host_path)
        for i, name in enumerate(names):
            try:
                if name in inode.entries:
                    next_path = inode.entries[name]
                elif create and i == len(names) - 1:
                    next_path = host_path + '/' + name
                    with open(next_path, 'w'):
                        pass
                    inode.entries[name] = next_path
                else:
                    raise Exception(f'Lookup {name} failed')
            finally:
                self.put_inode(inode)

            host_path = next_path
            inode = self.find_inode(host_path)

        try:
            yield inode
        finally:
            self.put_inode(inode)

    @trace
    def readdir(self, path: str) -> List[str]:
        with self.lookup(path) as inode:
            return list(inode.entries.keys())

    @trace
    def stat(self, path: str) -> dict:
        with self.lookup(path) as inode:
            if inode.is_dir:
                return {'type': 'dir'}
            else:
                with inode.size_lock:
                    return {'type': 'file', 'size': inode.size}

    @trace
    def open(self, path: str, create=False, append=False) -> int:
        with self.lookup(path, create=create) as inode:
            assert not inode.is_dir
            handle, fd = self.create_handle(inode, append)
            return fd

    @trace
    def read(self, fd, length):
        handle = self.handles[fd]
        with handle.pos_lock:
            with open(handle.inode.host_path, 'rb') as f:
                f.seek(handle.pos)
                result = f.read(length)
            random_sleep()
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
        if handle.append:
            with handle.inode.size_lock:
                result = self.do_write(handle.inode, handle.inode.size, data)
                handle.inode.size += result
        else:
            with handle.pos_lock:
                result = self.do_write(handle.inode, handle.pos, data)
                handle.pos += result
        return result

    def do_write(self, inode, pos, data):
        with open(inode.host_path, 'r+b') as f:
            f.seek(pos)
            result = f.write(data)
        random_sleep()
        return result

    @trace
    def close(self, fd):
        with self.global_lock:
            handle = self.handles[fd]
            del self.handles[fd]
        self.put_handle(handle)


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
        fs = FS(d)

        repeat(writer, [fs], 3)

        fd = fs.open('/log.txt')
        fs.seek(fd, fs.stat('/log.txt')['size'])
        repeat(writer_fd, [fs, fd], 3)

        fd = fs.open('/log.txt')
        result = fs.read(fd, 4096)
        print(result.decode())

    with tempfile.TemporaryDirectory() as d:
        fs = FS(d)


if __name__ == '__main__':
    main()
