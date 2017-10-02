import asyncio
import logging
import os
import socket
import struct
import subprocess
import tempfile
from asyncio.subprocess import Process
from typing import Dict, Tuple, Optional, List

from rbd2qcow2.libc_wrappers import set_pdeathsig

NBD_CMD_READ = 0
NBD_CMD_WRITE = 1
NBD_CMD_DISCONNECT = 2
NBD_CMD_FLUSH = 3
NBD_CMD_TRIM = 4
NBD_CMD_CACHE = 5
NBD_CMD_WRITE_ZEROES = 6

NBD_FLAG_HAS_FLAGS = 1
NBD_FLAG_READ_ONLY = 2
NBD_FLAG_SEND_FLUSH = 4
NBD_FLAG_SEND_FUA = 8
NBD_FLAG_ROTATIONAL = 16
NBD_FLAG_SEND_TRIM = 32
NBD_FLAG_SEND_WRITE_ZEROES = 64
NBD_FLAG_SEND_DF = 128
NBD_FLAG_CAN_MULTI_CONN = 256
NBD_FLAG_SEND_BLOCK_STATUS = 512
NBD_FLAG_SEND_RESIZE = 1024

NBD_REQUEST_MAGIC = 0x25609513
NBD_REPLY_MAGIC = 0x67446698

log = logging.getLogger(__name__)


# https://github.com/yoe/nbd/blob/master/doc/proto.md
class RemoteNBDError(Exception):
    pass


# FIXME: set O_NDLEAY on TCP sockets
class NBDClient:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, flags: int,
                 process: Process):
        self._writer = writer
        self._handle = 0
        self._flags = flags
        self._tasks = dict()  # type: Dict[int, Tuple(asyncio.Future, int)]
        self._disconnect_was_sent = False
        self._process = process  # steal reference counter

        if not (self._flags & NBD_FLAG_SEND_WRITE_ZEROES):
            log.warning('Writing of zeroes is not supported by NBD server. Simulating using generic write.')

        self._reader_task = asyncio.ensure_future(self._response_reader(reader))

    async def _enqueue_command(self, request_type: int, offset: int = 0, length: int = 0, data: bytes = None,
                               flags: int = 0) -> Optional[bytes]:
        if self._disconnect_was_sent:
            raise RuntimeError('Disconnect was sent, so new submission commands is disallowed')

        if offset % 512:
            raise ValueError("offset=%u is not a multiple of 512." % offset)

        expected_length = 0
        if request_type == NBD_CMD_WRITE:
            if not data:
                raise ValueError('It is not allowed to write empty chunks')
            if length:
                raise ValueError('It is not allowed to specify length directly')
            length = len(data)
        elif request_type == NBD_CMD_READ:
            if data:
                raise ValueError('Read operation MUST NOT not send AUX data')
            expected_length = length
            if not length:
                raise ValueError('It is not allowed to read empty chunks.')
        elif request_type == NBD_CMD_WRITE_ZEROES:
            if not length:
                raise ValueError('It is not allowed to zero-out empty areas.')
        elif request_type == NBD_CMD_DISCONNECT:
            self._disconnect_was_sent = True  # stop command enqueuing

        if length % 512:
            raise ValueError("length=%u is not a multiple of 512." % length)

        self._handle += 1
        header = struct.pack(
            '>LHHQQL',
            NBD_REQUEST_MAGIC,
            flags,
            request_type,
            self._handle,
            offset,
            length,
        )

        if request_type == NBD_CMD_DISCONNECT:
            self._writer.write(header)
            # TODO: shutdown(SHUT_WR)
            return

        fut = asyncio.Future()
        handle = self._handle

        self._tasks[handle] = (fut, expected_length)

        self._writer.write(header)
        if data:
            self._writer.write(data)

        # http://bugs.python.org/issue29930
        # await self._writer.drain()
        return await fut

    async def _response_reader(self, reader: asyncio.StreamReader):
        try:
            while True:
                try:
                    response_header = await reader.readexactly(4 + 4 + 8)
                except asyncio.IncompleteReadError as err:
                    if err.partial:
                        raise RuntimeError('Partial bytes was read.')
                    log.debug('Clean end of stream.')
                    break
                (magic, errno, handle) = struct.unpack(">LLQ", response_header)
                if magic != NBD_REPLY_MAGIC:
                    raise RuntimeError('Protocol error')
                # log.debug('Found response')
                (fut, expected_length) = self._tasks.pop(handle)
                if errno:
                    # TODO: some errno MUST broke connection (!) like request format error (!)
                    fut.set_exception(RemoteNBDError('Remote NBD Error', errno))
                    continue

                if not expected_length:
                    fut.set_result(None)
                    continue

                try:
                    fut.set_result(await reader.readexactly(expected_length))
                except Exception as e:
                    # TODO: set_exception(RemoteRBDError() from e)
                    fut.set_exception(RemoteNBDError('Remote NBD Error: Can\'t read associated data: %r' % e))
                    raise
        finally:
            if self._tasks:
                log.error('Incomplete tasks in the queue. Aborting them.')
                for (fut, expected_length) in self._tasks.values():
                    try:
                        fut.set_exception(RemoteNBDError('Protocol error', None))
                    except Exception:
                        # may be already cancelled.
                        pass
                self._tasks.clear()

    async def write(self, offset: int, data: bytes):
        if self._flags & NBD_FLAG_READ_ONLY:
            raise RuntimeError('Remote disk is read-only')
        # log.debug('Writing %d bytes of data at offset %d.', len(data), offset)
        await self._enqueue_command(NBD_CMD_WRITE, offset, data=data)

    async def write_zeroes(self, offset: int, length: int):
        if self._flags & NBD_FLAG_READ_ONLY:
            raise RuntimeError('Remote disk is read-only')

        if not (self._flags & NBD_FLAG_SEND_WRITE_ZEROES):
            await self.write(offset, b'\x00' * length)
            return

        # log.debug('Writing %d zeroes at offset %d.', length, offset)
        await self._enqueue_command(NBD_CMD_WRITE_ZEROES, offset, length=length)

    async def read(self, offset: int, length: int) -> bytes:
        return await self._enqueue_command(NBD_CMD_READ, offset, length)

    async def flush(self):
        log.debug('Flushing.')
        if self._flags & NBD_FLAG_READ_ONLY:
            raise RuntimeError('Remote disk is read-only')
        if not (self._flags & NBD_FLAG_SEND_FLUSH):
            raise RuntimeError('NBD server does not support flushing.')
        await self._enqueue_command(NBD_CMD_FLUSH)

    async def trim(self, offset: int, length: int):
        if self._flags & NBD_FLAG_READ_ONLY:
            raise RuntimeError('Remote disk is read-only')

        if not (self._flags & NBD_FLAG_SEND_TRIM):
            raise RuntimeError('TRIM is not supported.')

        log.debug('Trimming %d bytes at offset %d.', length, offset)
        await self._enqueue_command(NBD_CMD_TRIM, offset, length=length)

    async def quit(self):
        log.debug('Disconnecting.')
        # TODO: Read spec about outstanding tasks (!)
        await self._enqueue_command(NBD_CMD_DISCONNECT)
        await self._reader_task  # do not propogate exception here
        self._reader_task = None
        self._writer.close()
        self._writer = None
        await self._process.wait()
        self._process = None

    # TODO: what if abort is called during working quit() ? during another abort ?
    async def abort(self):
        log.debug('Aborting NBD.')
        self._disconnect_was_sent = True  # stop command enqueuing
        if self._writer is not None:
            self._writer.close()  # close socket, so reader will exit too.
            self._writer = None
        if self._reader_task is not None:
            try:
                await self._reader_task
            except Exception as e:
                log.debug('Reader task completed: %r.', e)
            self._reader_task = None
        assert not self._tasks
        if self._process is not None:
            try:
                self._process.kill()
            except ProcessLookupError:
                pass
            except Exception as e:
                log.debug('NBD-process killing failed: %r.', e)
            try:
                await self._process.wait()
            except Exception as e:
                log.debug('Qemu-nbd process completed: %r.', e)
            self._process = None


def my_preexec():
    set_pdeathsig()
    os.setsid()  # To ignore SIGINT sent to the process group.


_nbd_args = None


def get_optional_nbd_args() -> List[str]:
    global _nbd_args
    if _nbd_args is not None:
        return _nbd_args
    log.debug('Detecting which optional features are supported in qemu-nbd.')
    help = subprocess.check_output(['qemu-nbd', '--help'], stderr=subprocess.STDOUT)
    base_args = []  # type: List[str]
    if b'--discard=' in help:
        log.info('Discard is supported by qemu-nbd.')
        base_args.append('--discard=unmap')
        if b'--detect-zeroes' in help:
            log.info('Zeroes detection is supported by qemu-nbd.')
            base_args.append('--detect-zeroes=unmap')
        else:
            log.warning('Zeroes detection is not supported by qemu-nbd.')
    else:
        log.warning('Discard is not supported by qemu-nbd.')

    return base_args


async def open_image(image_filename: str) -> NBDClient:
    sockpath = tempfile.mktemp(prefix='nbd_socket_')
    process = None
    try:
        log.info('Starting qemu-nbd for file %s and socket %s.', image_filename, sockpath)
        args = [
            # TODO: --read-only
            # TODO: --snapshot
            # TODO: --load-snapshot
            'qemu-nbd',
            '--format=qcow2',
            '--cache=unsafe',  # TODO: unknown BUG writeback is much slower even without any sync command (!)
            # /dev/fd/42 ? no, connect is not possible  to already opened socket..
            '--socket=%s' % sockpath,
        ]
        args.extend(get_optional_nbd_args())
        args.append(image_filename)
        process = await asyncio.create_subprocess_exec(*args, preexec_fn=my_preexec)

        log.debug('Waiting for unix socket to appear.')
        for i in range(20):
            if os.path.exists(sockpath):
                break
            await asyncio.sleep(0.1)  # TODO: await ANY of (proces death, sleep)

        if not os.path.exists(sockpath):
            raise RuntimeError('NBD server did no start.')

        log.info('Connecting to qemu-nbd.')

        (reader, writer) = await asyncio.open_unix_connection(sockpath)
        sk = writer.transport.get_extra_info('socket')
        sk.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 16 * 1024 * 1024)
        # sk.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUFFORCE, 16 * 1024 * 1024)
        sk.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16 * 1024 * 1024)
        # sk.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUFFORCE, 16 * 1024 * 1024)
        del sk

        os.unlink(sockpath)
        buf = await reader.readexactly(16)

        if buf == b'NBDMAGIC\x00\x00\x42\x02\x81\x86\x12\x53':
            buf = await reader.readexactly(8 + 4)
            (size, flags) = struct.unpack(">QL", buf)
            if not (flags & NBD_FLAG_HAS_FLAGS):
                raise RuntimeError('Flags not supported. VERY old NBD server ?')
            await reader.readexactly(124)
        elif buf == b'NBDMAGICIHAVEOPT':
            raise NotImplementedError('New-style negotiation is not implemented yet.')
        else:
            raise RuntimeError('Protocol error during negotiation: %s.', ' '.join(hex(i) for i in buf[8:]))

        return NBDClient(reader, writer, flags, process)
    except Exception:
        log.exception('Error while opening image:')
        try:
            os.unlink(sockpath)
        except FileNotFoundError:
            pass
        if process is not None:
            try:
                process.kill()  # TODO: except no such process...
            except ProcessLookupError:
                pass
            except Exception as e:
                log.debug('Killing of process failed: 5r.', e)
            await process.wait()  # TODO: will it raise exception on non-zero exit code ?
        raise
