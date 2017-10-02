import asyncio
import logging
import threading
import time
from typing import List, Tuple, Optional

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
        #     'RBD transfer delay: %2.2f msec. %2.2f MB.',
        #     (time.monotonic() - start) * 1000,
        #     len(rbd_data) / 1000000
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
        self.jobs_completed = 0
        self.loop = loop
        self.total = 0
        self.total_bytes = 0
        self.prev_report_time = None
        self.rbd_image = rbd_image
        self.nbd_client = nbd_client
        self._transfers = set()
        self._reset_stat()

    async def _wait_for_transfers(self, return_when: str):
        (done, self._transfers) = await asyncio.wait(self._transfers, return_when=return_when)
        for future in done:  # type: asyncio.Future
            if future.cancelled():
                raise RuntimeError('Transfer cancelled.')
            if future.exception() is not None:
                raise RuntimeError('Transfer failed') from future.exception()
            (bytes_transferred, rbd_info, nbd_info) = future.result()
            self.total_bytes += bytes_transferred
            self.stat_total_bytes += bytes_transferred

            if rbd_info is not None:
                self.stat_rbd_stats.append(rbd_info)
            if nbd_info is not None:
                self.stat_nbd_stats.append(nbd_info)

        self.jobs_completed += len(done)
        self.stat_completed += len(done)

        percentage = self.jobs_completed * 100 // self.total
        now = time.monotonic()

        # Do not report too frequently (max once per 5 sec)
        if (now - self.prev_report_time < 5) and percentage < 100:
            return

        delta_time = now - self.prev_report_time

        log.info(
            'Completed: %d%%: %2.2f GB, %2.2f MB/sec, %2.2f IOPS, avg lat %2.2f msec, avg IO size %2.2f KB, %d IOs.',
            percentage,
            self.total_bytes / 1000000000,
            self.stat_total_bytes / (delta_time * 1000000),  # Whole speed (MB/sec)
            self.stat_completed / delta_time,  # WHOLE iops
            delta_time * 1000 / self.stat_completed,  # AVG latency (ms)
            self.stat_total_bytes / (self.stat_completed * 1000),  # Avg chunk size (KB)
            self.stat_completed,  # Chunks
        )

        if self.stat_rbd_stats:
            self._detailed_report(self.stat_rbd_stats, 'RBD')
        if self.stat_nbd_stats:
            self._detailed_report(self.stat_nbd_stats, 'NBD')
        self.prev_report_time = now
        self._reset_stat()

    @staticmethod
    def _detailed_report(stats: List[Tuple[int, float, float]], name: str):
        total_bytes = sum(i[0] for i in stats)
        total_ops = len(stats)
        summ_of_time = sum((i[2] - i[1]) for i in stats)

        # Calculate time of REAL transfers
        stats.sort(key=lambda i: i[1])  # sort by start time
        current_end = 0
        transfer_time = 0
        for (unused, start, end) in stats:
            if start > current_end:
                current_end = end
                transfer_time += end - start
            elif end > current_end:
                transfer_time += end - current_end
                current_end = end
        log.info(
            '%s ops for %2.2f sec: %2.2f MB/sec, %2.2f IOPS, avg lat %2.2f msec, avg IO size %2.2f KB, %d IOs.',
            name,
            transfer_time,
            total_bytes / (transfer_time * 1000000),  # RBD speed
            total_ops / transfer_time,  # RBD IOPS
            summ_of_time * 1000 / total_ops,  # AVG latency (ms)
            total_bytes / (total_ops * 1000),  # Avg chunk size
            total_ops,  # Chunks
        )

    def _reset_stat(self):
        self.stat_completed = 0
        self.stat_total_bytes = 0
        self.stat_rbd_stats = []
        self.stat_nbd_stats = []

    async def _transfer_chunk(self, offset: int, length: int, exists: bool) -> \
            Tuple[int, Optional[Tuple[float, float]], Tuple[float, float]]:
        if exists:
            time1 = time.monotonic()
            data = await rbd_read(self.loop, self.rbd_image, offset, length)
            time2 = time.monotonic()
            await self.nbd_client.write(offset, data)
            time3 = time.monotonic()
            return length, (length, time1, time2), (length, time2, time3)
        else:
            time2 = time.monotonic()
            await self.nbd_client.write_zeroes(offset, length)
            time3 = time.monotonic()
            return length, None, (length, time2, time3)

    async def _transfer(self, rbd_read_operations: List[Tuple[int, int, bool]], parallel: int):
        log.info('Transferring image with %d parallel stream(s).', parallel)
        self.total = len(rbd_read_operations)
        self.prev_report_time = time.monotonic()
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
