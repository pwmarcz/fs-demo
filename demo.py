import os
import shutil
from contextlib import ExitStack
from threading import Thread
import sys

from fs import FS, Mount
from sync_client import SyncClient
from sync_server import SyncServer
from util import setup_logging, random_sleep


def repeat(target, n):
    threads = [Thread(target=target) for i in range(n)]
    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()


def simple(fs: FS):
    """
    Simple sanity check for dentry invalidation.
    """

    with fs.cloned() as fs2:
        fs.readdir('/')              # no hello.txt
        fs.stat('/hello.txt')        # None
        fs2.stat('/hello.txt')

        fd = fs.open('/hello.txt')
        fs.stat('/hello.txt')        # size = 0
        fs2.stat('/hello.txt')

        fs.write(fd, b'hello')
        fs.stat('/hello.txt')        # size = 5
        fs2.stat('/hello.txt')

        fs.readdir('/')              # hello.txt present
        fs2.readdir('/')


def append(fs_: FS):
    """
    Several clients writing to the same file in append mode.
    """

    def writer():
        with fs_.cloned() as fs:
            fd = fs.open('/log.txt', append=True)
            for i in range(10):
                fs.write(fd, f'log line {i}\n'.encode())
                random_sleep()

    repeat(writer, 3)
    fd = fs_.open('/log.txt')
    data = fs_.read(fd, 2048)
    print(data.decode(), end='')


def shared_handle(fs_: FS):
    """
    Several clients writing to a shared file handle.
    """

    fd = fs_.open('/log.txt')

    def writer():
        with fs_.cloned() as fs:
            for i in range(10):
                fs.write(fd, f'log line {i}\n'.encode())
                random_sleep()

    repeat(writer, 3)
    fs_.seek(fd, 0)
    data = fs_.read(fd, 2048)
    print(data.decode(), end='')


def main():
    demos = {
        'simple': simple,
        'append': append,
        'shared_handle': shared_handle,
    }

    if len(sys.argv) != 2 or sys.argv[1] not in demos:
        print(f'Usage: {sys.argv[0]} demo_name')
        print('Demo names:', ', '.join(demos.keys()))
        print('(read demo.py for details)')
        sys.exit(1)

    demo = demos[sys.argv[1]]

    setup_logging()

    temp_dir = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'tmp')
    shutil.rmtree(temp_dir)
    os.mkdir(temp_dir)
    socket_path = temp_dir + '/server.sock'

    with ExitStack() as exit:
        # Start server
        server = SyncServer(socket_path)
        server_thread = Thread(name='Server', target=server.run)
        server_thread.start()
        exit.callback(server_thread.join)
        exit.callback(server.socket_server.shutdown)

        # Start client
        client = SyncClient(socket_path)
        client.start()
        exit.callback(client.stop)

        # Create objects
        mount = Mount('tmp', temp_dir)
        fs = FS(client, mount)

        # Run demo
        demo(fs)


if __name__ == '__main__':
    main()
