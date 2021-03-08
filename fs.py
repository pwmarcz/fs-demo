import os
import traceback
from contextlib import contextmanager
from threading import Thread
from typing import IO, Optional, ContextManager, List, Dict
from enum import Enum

from sync_client import SyncClient, SyncHandle
from util import random_sleep, trace, setup_logging


def split_path(path: str) -> List[str]:
    # Assume relative paths also begin at root, so we can strip leading /.
    path = path.lstrip('/')
    if path == '':
        return []
    else:
        return path.split('/')


class FileType(Enum):
    REGULAR = 1
    DIRECTORY = 2


class Dentry:
    def __init__(self,
                 mount: 'Mount',
                 path: str,
                 host_path: str,
                 sync: SyncHandle):
        self.mount = mount
        self.path = path
        self.host_path = host_path

        self.key = f'{self.mount.key}:{self.path}'

        # Protected by sync handle: take it before accessing
        self.file_type: Optional[FileType] = None
        self.size: int = 0

        self.sync = sync

    def locked(self):
        return self.sync.user_lock.locked()

    def __repr__(self):
        return f'<Dentry: {self.path!r}>'


class Handle:
    def __init__(self,
                 mount: 'Mount',
                 dentry: Dentry,
                 file: IO[bytes],
                 append: bool,
                 sync: SyncHandle):
        self.mount = mount
        self.dentry = dentry
        self.append = append
        self.file = file

        # Protected by sync handle: take it before accessing
        self.pos: int = 0

        self.sync = sync

    def __repr__(self):
        return f'<Handle: {self.file.name}>'


class Mount:
    def __init__(self, key: str, root_path: str, client: SyncClient):
        self.key = key
        self.root_path = root_path
        self.client = client

    @trace
    def create_dentry(self, path: str):
        sync = SyncHandle(self.client, f'dentry:{self.key}:{path}')
        dentry = Dentry(
            mount=self,
            path=path,
            host_path=self.root_path + '/' + path,
            sync=sync)
        return dentry

    @trace
    def load_dentry(self, dentry: Dentry):
        assert dentry.locked()
        if not os.path.exists(dentry.host_path):
            dentry.size = None
            dentry.file_type = None
        elif os.path.isdir(dentry.host_path):
            dentry.size = 0
            dentry.file_type = FileType.DIRECTORY
        else:
            dentry.size = os.stat(dentry.host_path).st_size
            dentry.file_type = FileType.REGULAR

    @trace
    def readdir(self, dentry: Dentry):
        return os.listdir(dentry.host_path)

    @trace
    def truncate(self, dentry: Dentry, size: int):
        os.truncate(dentry.host_path, size)

    @trace
    def create_file(self, dentry: Dentry, key: str, append: bool) -> Handle:
        assert dentry.file_type is None
        file = open(dentry.host_path, 'w+b')
        sync = SyncHandle(self.client, key, 0)
        handle = Handle(self, dentry, file, append, sync)
        return handle

    @trace
    def open_file(self, dentry: Dentry, key: str, append: bool) -> Handle:
        assert dentry.file_type == FileType.REGULAR
        file = open(dentry.host_path, 'r+b')
        sync = SyncHandle(self.client, key, 0)
        handle = Handle(self, dentry, file, append, sync)
        return handle

    @trace
    def write(self, handle: Handle, pos: int, data: bytes):
        return os.pwrite(handle.file.fileno(), data, pos)


class FS:
    def __init__(self, pid: int, client: SyncClient, root_mount: Mount):
        self.pid = pid
        self.client = client
        self.counter = 0
        self.root_mount = root_mount

        self.root: Dentry = root_mount.create_dentry('')
        self.dentry_cache = {self.root.key: self.root}
        self.handles: Dict[int, Handle] = {}

    @contextmanager
    def cloned(self) -> ContextManager['FS']:
        """
        Create a copy of this filesystem, emulating what happens after fork.
        """

        client = SyncClient(self.client.path)
        client.start()
        try:
            root_mount = Mount(self.root_mount.key, self.root_mount.root_path, client)
            fs = FS(pid=self.pid+1, client=client, root_mount=root_mount)
            yield fs
        finally:
            client.stop()

    def _load_dentry(self, dentry: Dentry, shared: bool):
        """
        Lock a dentry and ensure it's valid (reloaded after modification).
        """
        modified = dentry.sync.acquire(shared)
        try:
            if modified:
                dentry.mount.load_dentry(dentry)
        except Exception:
            dentry.sync.release()
            raise

    @contextmanager
    def _get_dentry(self, dentry: Dentry, shared: bool):
        self._load_dentry(dentry, shared)
        try:
            yield
        finally:
            dentry.sync.release()

    @contextmanager
    def _find_dentry(self, path: str) -> ContextManager[Dentry]:
        names = split_path(path)

        dentry = self.root
        self._load_dentry(dentry, shared=True)
        for i, name in enumerate(names):
            try:
                if dentry.file_type is None:
                    raise Exception(f'does not exist: {dentry.path}')
                if dentry.file_type is not FileType.DIRECTORY:
                    raise Exception(f'not a directory: {dentry.path}')
            finally:
                dentry.sync.release()

            next_path = dentry.path + '/' + name
            key = f'{dentry.mount.key}:{next_path}'
            if key in self.dentry_cache:
                dentry = self.dentry_cache[key]
            else:
                dentry = self.root_mount.create_dentry(next_path)
                self.dentry_cache[key] = dentry
            self._load_dentry(dentry, shared=True)

        try:
            yield dentry
        finally:
            dentry.sync.release()

    @trace
    def readdir(self, path):
        with self._find_dentry(path) as dentry:
            if dentry.file_type is None:
                raise Exception(f'does not exist: {dentry.path}')
            if dentry.file_type != FileType.DIRECTORY:
                raise Exception(f'not a directory: {dentry.path}')
            return dentry.mount.readdir(dentry)

    @trace
    def truncate(self, path, size):
        with self._find_dentry(path) as dentry:
            if dentry.file_type is None:
                raise Exception(f'does not exist: {dentry.path}')
            if dentry.file_type != FileType.REGULAR:
                raise Exception(f'not a regular file: {dentry.path}')
            dentry.mount.truncate(dentry, size)
            dentry.size = size
            dentry.sync.modified = True

    @trace
    def open(self, path, append=False):
        with self._find_dentry(path) as dentry:
            fd = 0
            while fd in self.handles:
                fd += 1
            key = f'handle:{self.pid}:{fd}'

            if dentry.file_type is None:
                handle = dentry.mount.create_file(dentry, key, append)
                dentry.file_type = FileType.REGULAR
                dentry.size = 0
                dentry.sync.modified = True
            else:
                handle = dentry.mount.open_file(dentry, key, append)

            self.handles[fd] = handle
            return fd

    @trace
    def write(self, fd: int, data: bytes):
        handle = self.handles[fd]
        dentry = handle.dentry
        if handle.append:
            # In append mode, lock the dentry and use its size as position.
            with self._get_dentry(dentry, shared=False):
                result = dentry.mount.write(handle, dentry.size, data)
                if result > 0:
                    dentry.size += result
                    dentry.sync.modified = True
        else:
            # In non-append mode, lock the handle and use its position.
            with handle.sync.get_exclusive():
                handle.pos = handle.sync.data
                result = handle.dentry.mount.write(handle, handle.pos, data)
                end = handle.pos + result
                if result > 0:
                    handle.pos = end
                    handle.sync.data = handle.pos
                    handle.sync.modified = True

            # Update dentry size, if necessary.
            if result > 0:
                with self._get_dentry(dentry, shared=False):
                    if dentry.size < end:
                        dentry.size = end
                        dentry.modified = True

        return result

    @trace
    def stat(self, path):
        with self._find_dentry(path) as dentry:
            if dentry.file_type is None:
                return None
            return {
                'type': dentry.file_type.name,
                'size': dentry.size,
            }


def main():
    setup_logging()

    client = SyncClient('server.sock')
    client.start()
    mount = Mount('tmp', '/tmp', client)
    fs = FS(os.getpid(), client, mount)
    fd = fs.open('/foo.txt', append=True)
    fs.write(fd, b'hello\n')
    fs.write(fd, b'world\n')


if __name__ == '__main__':
    main()
