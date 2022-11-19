import errno
import logging
import os
import signal
import time
from functools import wraps

if "DEBUG" not in globals():
    DEBUG = False

logging.basicConfig(format="%(asctime)s %(message)s")


def timeit(some_func):
    """A wrapper function to tell you how long a function took to run."""

    @wraps(some_func)
    def wrapper(*args, **kwargs):
        t1 = time.time()
        foo = some_func(*args, **kwargs)
        diff = time.time() - t1
        logging.debug(
            "%s completed in %.5f seconds." % (some_func.__name__, diff)
        )
        return foo

    return wrapper


def _VmB(VmKey: str = "VmRSS:") -> int:
    """A function to return, in bytes, how much RAM the current
    running process is using."""
    _scale = {
        "kB": 1024.0,
        "mB": 1024.0 * 1024.0,
        "KB": 1024.0,
        "MB": 1024.0 * 1024.0,
    }
    # get pseudo file  /proc/<pid>/status
    try:
        t = open("/proc/%d/status" % os.getpid())
        v = t.read()
        t.close()
        del t
    except IOError:
        return 0  # non-Linux?
    # get VmKey line e.g. 'VmRSS:  9999  kB\n ...'
    i = v.index(VmKey)
    v = v[i:].split(None, 3)  # whitespace
    del i
    if len(v) < 3:
        del v
        return 0  # invalid format?
        # convert Vm value to bytes
    return round(float(v[1]) * _scale[v[2]])


def Daemonize() -> None:
    """Daemonize a process in Python 2.4+"""
    import resource

    pid = os.fork()
    if pid == 0:  # Are we the child?
        os.setsid()  # Create a new session
        signal.signal(signal.SIGHUP, signal.SIG_IGN)  # Ignore SIGHUP
        pid = os.fork()  # Fork again
        if pid == 0:  # Are we the new child?
            os.umask(0)  # Clear any UMASK flags that were set.
        else:  # Exit the first child.
            os._exit(0)
    else:  # Exit the parent.
        os._exit(0)

    # Ask the OS how many open FDs there can be.
    maxfd = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
    if maxfd == resource.RLIM_INFINITY:
        maxfd = 1024  # But use a sane value if the OS is confused.

    # Close all open file descriptors.
    for fd in range(0, maxfd):
        try:
            os.close(fd)
        except OSError:  # Any unopened fd's will error.
            pass

    # Use the OS defined "/dev/null" if exists.
    try:
        from subprocess import DEVNULL as devnull
    except ImportError:
        from os import devnull
    os.open(devnull, os.O_RDWR)  # Redirect stdin to /dev/null
    os.dup2(0, 1)  # Redirect stdout to /dev/null
    os.dup2(0, 2)  # Redirect stderr to /dev/null
    return


class TimeoutError(Exception):
    pass


def timeout(
    seconds: float = 10.0, error_message: str = os.strerror(errno.ETIME)
):
    """A wrapper function to allow you to specify a timeout value for any function.
    Usage:
    @timeout(2.5, 'Oops, you broke it')
    def myFunc(some, args):
        try:
            do_something_that_might_hang()
        except TimeoutError as TO:
            logging.debug("It timed out: %s" % str(TO))"""

    def todec(func):
        def _timedOut(signum, frame):
            raise TimeoutError(error_message)

        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, _timedOut)
            signal.setitimer(signal.ITIMER_REAL, seconds)
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return result

        return wraps(func)(wrapper)

    return todec
