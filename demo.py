import os
from typing import List, Dict, ContextManager
import tempfile
from contextlib import contextmanager

from client import Client, LocalClient, Object


class Inode(Object):
    def __init__(self, key: str, entries: Dict[str, str], size: int):
        super().__init__(key)
        self.host_path = key
        self.entries = entries
        self.size = size

    def checkpoint(self):
        return {'entries': self.entries, 'size': self.size}

    @classmethod
    def restore_new(cls, key, data):
        return cls(key, data['entries'], data['size'])

    def restore(self, data):
        self.entries = data['entries']
        self.size = data['size']


class FS:
    def __init__(self, client: Client, root_path: str):
        self.client = client
        self.root_path = root_path

    def get_inode(self, host_path: str):
        inode = self.client.get(host_path, Inode)
        if not inode:
            inode = self.create_inode(host_path)
        return inode

    def create_inode(self, host_path):
        if os.path.isdir(host_path):
            entries = {
                name: host_path + '/' + name
                for name in os.listdir(host_path)
            }
            size = 0
        else:
            entries = {}
            size = os.stat(host_path).st_size
        inode = Inode(host_path, entries, size)
        self.client.register(inode)
        return inode

    @contextmanager
    def lookup(self, path: str, create: bool = False) -> ContextManager[Inode]:
        if path == '/':
            names = []
        else:
            names = path.lstrip('/').split('/')

        host_path = self.root_path
        inode = self.get_inode(host_path)
        for i, name in enumerate(names):
            try:
                if name in inode.entries:
                    next_path = inode.entries[name]
                elif create and i == len(names) - 1:
                    next_path = host_path + '/' + name
                    with open(next_path, 'w'):
                        pass
                    with self.client.write(inode):
                        inode.entries[name] = next_path
                else:
                    raise Exception(f'Lookup {name} failed')
            finally:
                self.client.put(inode)

            host_path = next_path
            inode = self.get_inode(host_path)

        try:
            yield inode
        finally:
            self.client.put(inode)

    def readdir(self, path: str) -> List[str]:
        with self.lookup(path) as inode:
            return list(inode.entries.keys())

    def stat(self, path: str) -> dict:
        with self.lookup(path) as inode:
            if inode.size < 0:
                return {'type': 'dir'}
            else:
                return {'type': 'file', 'size': inode.size}

    def readfile(self, path: str) -> bytes:
        with self.lookup(path) as inode:
            with open(inode.host_path, 'rb') as f:
                return f.read()

    def append(self, path: str, data: bytes) -> None:
        with self.lookup(path, create=True) as inode:
            with self.client.update(inode):
                with open(inode.host_path, 'r+b') as f:
                    f.seek(inode.size)
                    f.write(data)
                with self.client.write(inode):
                    inode.size += len(data)


def main():
    with tempfile.TemporaryDirectory() as d:
        fs = FS(LocalClient(), d)
        os.mkdir(d + '/subdir')

        print(fs.readdir('/'))
        print(fs.readdir('/subdir'))
        print(fs.stat('/subdir'))
        fs.append('/foo.txt', b'hello ')
        fs.append('/foo.txt', b'world')
        print(fs.stat('/foo.txt'))
        print(fs.readfile('/foo.txt'))


if __name__ == '__main__':
    main()
