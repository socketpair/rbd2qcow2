import asyncio
import logging
import threading
import time
from typing import List, Tuple

import rados
import rbd

from rbd2qcow2.nbd_client import NBDClient

# LIBRADOS_OP_FLAG_FADVISE_RANDOM ? SEQUENTAL ? NOTHING ?
RBD_FLAGS = rados.LIBRADOS_OP_FLAG_FADVISE_NOCACHE | rados.LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL

log = logging.getLogger(__name__)


async def rbd_read(loop, rbd_image: rbd.Image, offset: int, length: int) -> bytes:
    fut = asyncio.Future()

    # aio_read completion callback will fire in separate thread
    # it will return Completion object, which has .exc_info member.
    # we need to check it by a time .... .is_complete()
    # It seems, there is no way to abort operation...
    lock = threading.Lock()
    cancelled = False

    # start = time.monotonic()

    def rbd_async_callback(unused, rbd_data: bytes):
        """ WARNING! this function run in separate internal librados thread! """
        # loop.call_soon_threadsafe(
        #     log.debug,
        #     'RBD transfer delay: %2.2f msec. %2.2f MiB.',
        #     (time.monotonic() - start) * 1000,
        #     len(rbd_data) / (1024 * 1024)
        # )
        nonlocal cancelled
        with lock:
            if not cancelled:
                loop.call_soon_threadsafe(fut.set_result, rbd_data)

    rbd_image.aio_read(
        offset,
        length,
        rbd_async_callback,
        RBD_FLAGS
    )

    try:
        data = await fut
    except asyncio.CancelledError:
        # TODO: Unfortunatelly librbd HAS NO way to cancel AIO request.,
        # so if our future is cancelled it's nothing to do here...
        log.debug('Faking cancellation of RBD transfer.')
        with lock:
            cancelled = True
        raise
    if len(data) != length:
        # TODO: check at the end of an image!
        raise RuntimeError('Data length mismatch in RBD response', len(data), length)
    return data


class Transferrer:
    def __init__(
            self,
            loop: asyncio.AbstractEventLoop,
            rbd_image: rbd.Image,
            nbd_client: NBDClient
    ):
        self.completed = 0
        self.percentage = None
        self.loop = loop
        self.total = 0
        self.total_bytes = 0
        self.prev_report_time = None
        self.prev_report_size = 0
        self.rbd_image = rbd_image
        self.nbd_client = nbd_client
        self._transfers = set()

    async def _wait_for_transfers(self, return_when: str):
        (done, self._transfers) = await asyncio.wait(self._transfers, return_when=return_when)
        for future in done:  # type: asyncio.Future
            if future.cancelled():
                raise RuntimeError('Transfer was cancelled.')
            if future.exception() is not None:
                raise RuntimeError('Transfer was failed') from future.exception()
            self.total_bytes += future.result()
        self.completed += len(done)
        new_percentage = self.completed * 100 // self.total
        if new_percentage != self.percentage:
            now = time.monotonic()
            if self.prev_report_time is None:
                speed = 0
            else:
                speed = (self.total_bytes - self.prev_report_size) / ((now - self.prev_report_time) * 1024 * 1024)
            log.info(
                'Completed: %d%% (%2.2f GiB transferred). %2.2f MiB/sec.',
                new_percentage,
                self.total_bytes / (1024 * 1024 * 1024),
                speed,
            )
            self.percentage = new_percentage
            self.prev_report_time = now
            self.prev_report_size = self.total_bytes

    async def _transfer_chunk(self, offset: int, length: int, exists: bool):
        if exists:
            data = await rbd_read(self.loop, self.rbd_image, offset, length)
            await self.nbd_client.write(offset, data)
        else:
            await self.nbd_client.write_zeroes(offset, length)
        return length

    async def _transfer(self, rbd_read_operations: List[Tuple[int, int, bool]], parallel: int):
        log.info('Transferring image with %d parallel stream(s).', parallel)
        self.total = len(rbd_read_operations)
        for (offset, length, exists) in rbd_read_operations:
            while len(self._transfers) >= parallel:
                await self._wait_for_transfers(asyncio.FIRST_COMPLETED)

            # ensure_future is required since we need .cancel() method..
            self._transfers.add(asyncio.ensure_future(self._transfer_chunk(offset, length, exists)))

        log.debug('Iteration loop complete.')
        if self._transfers:
            log.debug('Waiting for the tail transfers.')
            await self._wait_for_transfers(asyncio.ALL_COMPLETED)

        log.debug('Flushing QCOW2 image')
        await self.nbd_client.flush()

    async def transfer(self, rbd_read_operations: List[Tuple[int, int, bool]], parallel: int = 1):
        try:
            await self._transfer(rbd_read_operations, parallel)
        except Exception as e:
            transfers = self._transfers
            if transfers:
                log.debug('Aborting chunk transfers due to transfer error: %r.', e)
                self._transfers = set()
                for t in transfers:
                    try:
                        t.cancel()
                    except Exception as e2:
                        log.debug('Chunk transfer cancellation failed: %r.', e2)
                # TODO: Should I await for cancelled tasks ?
                for t in transfers:
                    try:
                        await t
                    except Exception as e3:
                        log.debug('Chunk transfer was cancelled with exception: %r.', e3)
            raise
