import os

from ctypes import CDLL, c_int, get_errno, c_long
from signal import SIGKILL

PR_SET_PDEATHSIG = 1

libc = CDLL(None, use_errno=True)


def check_errno(result, func, args):
    if result != -1:
        return result
    errno = get_errno()
    raise OSError(errno, os.strerror(errno))


c_off_t = c_long  # TODO: actually, is not always so (!)
FALLOC_FL_KEEP_SIZE = 1
fallocate = libc.fallocate
fallocate.restype = c_int
fallocate.argtypes = [c_int, c_int, c_off_t, c_off_t]
fallocate.errcheck = check_errno


def set_pdeathsig(signal: int = SIGKILL):
    if libc.prctl(PR_SET_PDEATHSIG, signal) != 0:
        raise RuntimeError('Fail to set PDEATHSIG')
    if os.getppid() == 1:
        os.kill(os.getpid(), signal)
