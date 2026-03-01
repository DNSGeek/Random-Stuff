import errno
import logging
import os
import signal
import time
from collections.abc import Callable
from functools import wraps
from types import FrameType
from typing import Any, Optional, TypeVar

# FIX: removed the `if "DEBUG" not in globals()` guard — that pattern is a Python 2
# holdover that does nothing useful in a module context. If callers want to gate on
# DEBUG, they should configure the logging level directly (see logging.basicConfig below).

logging.basicConfig(format="%(asctime)s %(message)s")

# Generic type var so @timeit and @timeout preserve the wrapped function's signature
# for type checkers instead of collapsing it to (*args, **kwargs) -> Any.
F = TypeVar("F", bound=Callable[..., Any])


def timeit(some_func: F) -> F:
    """Decorator that logs how long the wrapped function took to run (at DEBUG level)."""

    @wraps(some_func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        # OPT: time.perf_counter() over time.time() — higher resolution monotonic
        # clock, not affected by system clock adjustments.
        t1: float = time.perf_counter()
        result: Any = some_func(*args, **kwargs)
        diff: float = time.perf_counter() - t1
        logging.debug(
            "%s completed in %.5f seconds.", some_func.__name__, diff
        )
        return result

    return wrapper  # type: ignore[return-value]


# Valid /proc/status memory scale suffixes → multiplier in bytes
_VM_SCALE: dict[str, float] = {
    "kB": 1024.0,
    "mB": 1024.0 * 1024.0,  # non-standard but seen in the wild
    "KB": 1024.0,
    "MB": 1024.0 * 1024.0,
}


def memory_usage(vm_key: str = "VmRSS:") -> int:
    """Return the current process's memory usage in bytes for the given /proc/status key.

    Defaults to VmRSS (resident set size). Returns 0 on non-Linux systems or on
    any parse failure.

    Common keys: VmRSS (resident), VmSize (virtual), VmPeak (peak virtual).
    """
    # OPT: use context manager — the original manually opened, read, closed, and
    # del'd the handle, which is exception-unsafe and verbose.
    try:
        with open(f"/proc/{os.getpid()}/status") as f:
            status: str = f.read()
    except OSError:
        # FIX: was catching IOError specifically; in Python 3 IOError is an alias
        # for OSError, but OSError is the correct base class to catch here.
        return 0  # non-Linux or permission denied

    try:
        # Find the key line, e.g. "VmRSS:  9999  kB\n"
        idx: int = status.index(vm_key)
        parts: list[str] = status[idx:].split(None, 3)
        if len(parts) < 3:
            return 0
        scale: float = _VM_SCALE.get(parts[2], 0.0)
        if scale == 0.0:
            return 0  # FIX: was KeyError-prone dict lookup; .get() with fallback is safe
        return round(float(parts[1]) * scale)
    except (ValueError, IndexError):
        return 0


# Keep the old private name as an alias so existing callers don't break.
_VmB = memory_usage


def daemonize() -> None:
    """Detach the current process from the terminal and run it as a daemon.

    Uses the standard UNIX double-fork technique to ensure the daemon cannot
    re-acquire a controlling terminal. Safe on Python 3 on any POSIX system.

    Note: subprocess.DEVNULL is available in Python 3.3+, so the fallback
    import of os.devnull has been removed.

    FIX: docstring previously said 'Python 2.4+' — updated to reflect Python 3.
    """
    import resource

    pid: int = os.fork()
    if pid == 0:  # First child
        os.setsid()  # Create a new session; detach from controlling terminal
        signal.signal(signal.SIGHUP, signal.SIG_IGN)  # Ignore SIGHUP
        pid = os.fork()  # Second fork: prevent re-acquiring a terminal
        if pid != 0:
            os._exit(0)  # Exit first child; grandchild continues
        os.umask(
            0
        )  # Clear umask so daemon can create files with any permissions
    else:
        os._exit(0)  # Exit the original parent

    # Close all open file descriptors so the daemon doesn't hold onto
    # inherited handles (sockets, pipes, log files, etc.)
    maxfd: int = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
    if maxfd == resource.RLIM_INFINITY:
        maxfd = 1024  # POSIX minimum; use as a safe fallback

    for fd in range(
        maxfd
    ):  # OPT: range(0, n) → range(n); 0 is the default start
        try:
            os.close(fd)
        except OSError:
            pass  # fd wasn't open; that's fine

    # FIX: removed try/except ImportError fallback for os.devnull — subprocess.DEVNULL
    # has been available since Python 3.3 and is simply the integer -3, not a path.
    # We need the actual /dev/null path string for os.open(), so use os.devnull directly.
    null_fd: int = os.open(
        os.devnull, os.O_RDWR
    )  # Opens as fd 0 (stdin → /dev/null)
    os.dup2(null_fd, 1)  # stdout → /dev/null
    os.dup2(null_fd, 2)  # stderr → /dev/null


# Keep the old capitalised name as an alias so existing callers don't break.
Daemonize = daemonize


# FIX: Python 3.3+ has a built-in TimeoutError (subclass of OSError). Redefining
# it as a plain Exception subclass shadows the built-in and is surprising to callers.
# We define our own distinct class so it doesn't collide, and document the distinction.
class TimedOutError(Exception):
    """Raised by the @timeout decorator when a function exceeds its time limit.

    Distinct from Python 3's built-in TimeoutError (an OSError subclass used for
    network/IO timeouts) to avoid masking unrelated OS-level timeout exceptions.
    """

    pass


def timeout(
    seconds: float = 10.0,
    error_message: str = os.strerror(errno.ETIME),
) -> Callable[[F], F]:
    """Decorator that raises TimedOutError if the wrapped function runs too long.

    Uses SIGALRM, so it only works on POSIX systems and only on the main thread.

    Usage:
        @timeout(2.5, 'That took way too long')
        def my_func(some, args):
            try:
                do_something_that_might_hang()
            except TimedOutError as e:
                logging.debug("Timed out: %s", e)

    FIX: inner functions were untyped and @wraps was applied manually at the end
    rather than as a decorator. Cleaned up and fully typed.

    SECURITY: signal-based timeouts are not thread-safe. Only use this decorator
    on the main thread. If you need thread-safe timeouts, use concurrent.futures
    with a ThreadPoolExecutor or ProcessPoolExecutor instead.
    """

    def decorator(func: F) -> F:
        def _timed_out(signum: int, frame: Optional[FrameType]) -> None:
            raise TimedOutError(error_message)

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            signal.signal(signal.SIGALRM, _timed_out)
            signal.setitimer(signal.ITIMER_REAL, seconds)
            try:
                return func(*args, **kwargs)
            finally:
                # FIX: the original called signal.alarm(0) to cancel — but since we
                # set a floating-point timer with setitimer, we must cancel with
                # setitimer too. signal.alarm() only works with integer seconds and
                # does not cancel an ITIMER_REAL set by setitimer on all platforms.
                signal.setitimer(signal.ITIMER_REAL, 0)

        return wrapper  # type: ignore[return-value]

    return decorator  # type: ignore[return-value]
