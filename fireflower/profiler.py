import logging
from functools import wraps

from contextlib import contextmanager
import time
import resource
import humanize
"""
Copied from signals profiler. Might be a good idea to consolidate this...
"""

KB_TO_BYTE = 1024


@contextmanager
def profiling(logger, key=None, scale=1, to_profile=True, **kwargs):
    """ contextmanager to log function call time """
    if not to_profile:
        yield
        return

    log_kwargs = kwargs

    if key:
        log_kwargs['key'] = key
        logger.debug('profiling_start', **log_kwargs)
    before_time = time.time()
    # on mac, it seems ru_maxrss returns byte, but on unix it seems to return KB
    before_max_mem_usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * KB_TO_BYTE
    try:
        yield
    finally:
        after_time = time.time()
        after_max_mem_usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * KB_TO_BYTE

        num_secs = float(after_time - before_time) / scale if scale else 0
        log_kwargs['num_secs'] = num_secs
        log_kwargs['pre_max_mem_used_bytes'] = before_max_mem_usage
        log_kwargs['post_max_mem_used_bytes'] = after_max_mem_usage
        log_kwargs['max_mem_used_bytes'] = after_max_mem_usage - before_max_mem_usage
        log_kwargs['max_mem_used'] = humanize.naturalsize(after_max_mem_usage - before_max_mem_usage)
        level = logging.INFO if num_secs > 0.1 else logging.DEBUG

        if level == logging.INFO:
            logger.info("profiling_end", **log_kwargs)
        elif level == logging.DEBUG:
            logger.debug("profiling_end", **log_kwargs)



def profile(logger, **profile_kwargs):
    """ decorator to log function call time """
    def decorator(f):
        @wraps(f)
        def with_profiling(*args, **kwargs):
            with profiling(logger, key=f.__name__, **profile_kwargs):
                return f(*args, **kwargs)
        return with_profiling
    return decorator


def profile_method(logger, **profile_kwargs):
    """ decorator to log (instance) method call time """
    def decorator(f):
        @wraps(f)
        def wrapper(self, *args, **kwargs):
            key = type(self).__name__ + '#' + f.__name__
            with profiling(logger, key=key, **profile_kwargs):
                return f(self, *args, **kwargs)

        return wrapper

    return decorator
