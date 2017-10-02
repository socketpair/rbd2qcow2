import argparse
import asyncio
import errno
import gc
import logging
import os
import re
import signal
import subprocess
import tempfile
import time
from typing import Tuple, List, Optional

import rados
import rbd

from rbd2qcow2.libc_wrappers import fallocate, FALLOC_FL_KEEP_SIZE
from rbd2qcow2.nbd_client import open_image
from rbd2qcow2.transferrer import Transferrer

log = logging.getLogger(__name__)


# TODO: protect rbd snapshots
# TODO: nbd + discard ? discard with backingstore_image? is it same as write_zeroes ?

# Supported qcow2 options:
# size             Virtual disk size
# compat           Compatibility level (0.10 or 1.1)
# backing_file     File name of a base image
# backing_fmt      Image format of the base image
# encryption       Encrypt the image
# cluster_size     qcow2 cluster size
# preallocation    Preallocation mode (allowed values: off, metadata, falloc, full)
# lazy_refcounts   Postpone refcount updates
# refcount_bits    Width of a reference count entry in bits


def create_qcow2_image(filename: str, size: int, backing_store_filename: str = None, backingstore_format='qcow2'):
    log.debug('Creating QCOW2 image %s of virtual size %2.2f GiB.', filename, size / (1024 * 1024 * 1024))
    args = ['qemu-img', 'create', '-f', 'qcow2']
    if backing_store_filename is not None:
        # TODO: escape commas
        args += [
            '-o',
            'backing_file={},backing_fmt={}'.format(backing_store_filename, backingstore_format)
        ]
    args += [filename, '{}B'.format(size)]
    log.debug('Calling qemu-img create.')
    subprocess.check_call(args)


def do_fallocate(filename: str, size: int):
    log.debug('Fallocating %s for size %d.', filename, size)
    fd = os.open(filename, os.O_RDWR)
    try:
        # os.posix_fallocate:
        # 1. does not support FALLOC_FL_KEEP_SIZE
        # 2. has bug: http://bugs.python.org/issue31106
        # 3. will emulate instead of raising of error if operation is not
        #    supported (see libc docs on posix_fallocate)
        fallocate(fd, FALLOC_FL_KEEP_SIZE, 0, size)
    except OSError as err:
        if err.errno != errno.EOPNOTSUPP:
            raise
        log.warning('Fallocate is not supported on your FS. Skipped.')
    finally:
        os.close(fd)


async def do_transfer(
        loop,
        ioctx,
        rbd_image_name: str,
        rbd_snap_name: str,
        base_rbd_snapshot: Optional[str],
        qcow2_name: str,
        backing_store_filename: Optional[str],
):
    log.debug('Opening RBD image %s (snap %r).', rbd_image_name, rbd_snap_name)
    with rbd.Image(ioctx, name=rbd_image_name, snapshot=rbd_snap_name, read_only=True) as rbd_image:
        log.info('Getting list of chunks to transfer.')
        size = rbd_image.size()
        log.info('RBD image size is %2.2f GiB.', size / (1024 * 1024 * 1024))
        rbd_read_operations = []  # type: List[Tuple[int, int, bool]]
        rbd_image.diff_iterate(0, size, base_rbd_snapshot, lambda *args: rbd_read_operations.append(args))

        if not rbd_read_operations:
            log.warning('Image %s was not changed since last backup. Skipping any transfers.', rbd_image_name)
            return

        size_nonzero = sum(i[1] for i in rbd_read_operations if i[2])
        if log.isEnabledFor(logging.INFO):
            cnt = len(rbd_read_operations)
            size_zero = sum(i[1] for i in rbd_read_operations if not i[2])
            log.info(
                'Need to transfer %d chunks: %2.2f GiB of data + %2.2f GiB of zeroes.',
                cnt,
                size_nonzero / (1024 * 1024 * 1024),
                size_zero / (1024 * 1024 * 1024),
            )

        qcow2_directory = os.path.dirname(os.path.realpath(qcow2_name))
        tmp_filename = tempfile.mktemp(prefix='qcow2_', dir=qcow2_directory)
        try:
            log.info('Creating image %s of virtual size %2.2f GiB.', qcow2_name, size / (1024 * 1024 * 1024))
            create_qcow2_image(tmp_filename, size, backing_store_filename)

            if size_nonzero:
                do_fallocate(tmp_filename, size_nonzero)

            nbd_client = await open_image(tmp_filename)
            try:
                transferrer = Transferrer(loop, rbd_image, nbd_client)
                await transferrer.transfer(rbd_read_operations, options.parallel)
            except Exception as e:
                log.error('Aborting NBD due to transfer error: %r.', e)
                await nbd_client.abort()
                raise

            log.debug('Fsyncing %s.', tmp_filename)
            # workaround for opening image as unsafe
            with open(tmp_filename, 'rb') as xxx:
                os.fsync(xxx.fileno())
            log.debug('Fsyncing complete.')

            os.chmod(tmp_filename, 0o400)
            os.rename(tmp_filename, qcow2_name)
            # Safe rename
            fd = os.open(qcow2_directory, os.O_DIRECTORY | os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
            log.debug('Terminating NBD connection.')
            await nbd_client.quit()  # TODO: CancelledError should be trapped with nbd_client.abort()
        except Exception as e:
            log.debug('Unlinking temporary image %r due to error: %r', tmp_filename, e)
            try:
                os.unlink(tmp_filename)
            except FileNotFoundError:
                pass
            raise


def get_latest_backup(xxx: str, image_name: str) -> int:
    log.debug('Searching for previous images in backup dir.')
    ts = 0
    for filename in os.listdir(xxx):
        mtch = re.fullmatch(r'([^@]+)@([0-9]+)\.qcow2', filename)
        if mtch is None:
            continue
        if mtch.group(1) != image_name:
            log.warning('Unexpected filename %r in dir %r.', filename, xxx)
            continue
        timestamp = int(mtch.group(2))
        if timestamp > ts:
            ts = timestamp
    return ts


async def do_backup(rbd_image_name: str, loop, ioctx):
    xxx = os.path.join(options.directory, rbd_image_name)
    if not os.path.isdir(xxx):
        log.debug('Creating directory %s.', xxx)
        os.makedirs(xxx)
    curr_ts = int(time.time())
    latest_ts = get_latest_backup(xxx, rbd_image_name)
    if latest_ts == 0:
        log.info('Did not found previous backup for image %s.', rbd_image_name)
        empty_image_path = os.path.join(xxx, 'empty.qcow2')
        if not os.path.exists(empty_image_path):
            log.info('Creating empty base qcow2 image.')
            create_qcow2_image(empty_image_path, 1024 * 1024)
        backing_store_filename = 'empty.qcow2'
        rbd_base_snapshot = None
    else:
        if latest_ts > curr_ts:
            raise RuntimeError('Detected wrong clocks.')
        backing_store_filename = '{}@{}.qcow2'.format(rbd_image_name, latest_ts)
        rbd_base_snapshot = str(latest_ts)
    qcow2_name = os.path.join(xxx, '{}@{}.qcow2'.format(rbd_image_name, curr_ts))
    rbd_new_snapshot_name = str(curr_ts)

    log.info('Creating RBD snapshot %s for image %s.', rbd_new_snapshot_name, rbd_image_name)
    with rbd.Image(ioctx, rbd_image_name) as rbd_image:
        rbd_image.create_snap(rbd_new_snapshot_name)
    try:
        await do_transfer(
            loop,
            ioctx,
            rbd_image_name,
            rbd_new_snapshot_name,
            rbd_base_snapshot,
            qcow2_name,
            backing_store_filename,
        )
    except Exception as e:
        log.info('Removing RBD snapshot %s@%s due to error %r.', rbd_image_name, rbd_new_snapshot_name,
                 e)
        with rbd.Image(ioctx, rbd_image_name) as rbd_image:
            rbd_image.remove_snap(rbd_new_snapshot_name)
        raise

    if options.remove_old_snapshots:
        log.info('Removing stale RBD snapshots.')
        with rbd.Image(ioctx, rbd_image_name) as rbd_image:
            for snap in list(rbd_image.list_snaps()):
                snap_name = snap['name']
                if snap_name == rbd_new_snapshot_name:
                    continue
                if not re.fullmatch(r'[0-9]+', snap_name):
                    continue
                log.info('Removing snapshot %r@%r.', rbd_image_name, snap_name)
                rbd_image.remove_snap(snap_name)


async def async_main(loop):
    log.debug('Attaching to CEPH cluster.')

    conf = dict()
    if options.keyring is not None:
        conf['keyring'] = options.keyring

    with rados.Rados(conffile='/etc/ceph/ceph.conf', rados_id=options.rados_id, conf=conf) as cluster:
        log.debug('Opening IO context for pool %s.', options.pool)
        with cluster.open_ioctx(options.pool) as ioctx:
            images = options.images
            if not images:
                log.info('Listing RBD images in pool %s.', options.pool)
                images = sorted(rbd.RBD().list(ioctx))
            for rbd_image_name in images:
                await do_backup(rbd_image_name, loop, ioctx)


def run_async_task(loop, coro):
    task = loop.create_task(coro)

    def signal_handler(*args):
        loop.remove_signal_handler(signal.SIGINT)
        loop.remove_signal_handler(signal.SIGTERM)
        log.info('SIGINT or SITERM caught. Aborting tasks.')
        task.cancel()

    loop.add_signal_handler(signal.SIGINT, signal_handler)
    loop.add_signal_handler(signal.SIGINT, signal_handler)
    loop.run_until_complete(task)


def main():
    global options

    parser = argparse.ArgumentParser(description='Incremental RBD => qcow2 backupper.')

    parser.add_argument(
        '--remove-old-snapshots',
        action='store_true',
        help='Remove all previously created snapshots (only those which was created by this tool).',
        default=False,
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable debug messages.',
        default=False,
    )
    parser.add_argument(
        '--parallel',
        type=int,
        metavar='COUNT',
        help='How many RBD transfers should be issued in parallel. By default, one transfer at a time.',
        default=1,
    )

    parser.add_argument(
        '--rados-id',
        type=str,
        metavar='ID',
        help='Rados ID, so real rados name will be client.<this id>.',
        default=None,
    )

    parser.add_argument(
        '--keyring',
        type=str,
        metavar='FILENAME',
        help='Filename with Rados keyring. Usually used altogether with --rados-id.',
        default=None,
    )

    parser.add_argument(
        'pool',
        metavar='POOL_NAME',
        type=str,
        help='The name of a CEPH pool to operate on.',
    )

    parser.add_argument(
        'directory',
        metavar='DEST_DIR',
        type=str,
        help='Path to a directory where backups shoul be placed.'
    )

    parser.add_argument(
        'images',
        metavar='IMAGE_NAME',
        type=str,
        nargs='*',
        help='An image name to backup. Backup all images if no image is specified.',
    )

    options = parser.parse_args()
    if not (1 <= options.parallel <= 100):
        raise ValueError('Wrong parallel count.')

    logging.basicConfig(level=logging.DEBUG if options.verbose else logging.INFO)

    log.info('Starting backup process.')
    loop = asyncio.get_event_loop()
    try:
        run_async_task(loop, async_main(loop))
        gc.collect()  # garbage collect complete tasks
        for t in asyncio.Task.all_tasks(loop):
            log.debug('BUG: Incomplete tasks: %r.', t)
    finally:
        loop.close()
        log.info('Backup complete.')


if __name__ == '__main__':
    main()
