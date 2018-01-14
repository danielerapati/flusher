from functools import partial
from time import time


def log_instrumented(func, logfunc):

    def wrapped(*args, **kwargs):
        if args or kwargs:
            logfunc('{}: called with arguments {} {}'.format(
                    func.__name__,
                    (args if args else ''),
                    (kwargs if kwargs else '')))
        else:
            logfunc('{} called'.format(func.__name__))

        start = time()
        res = func(*args, **kwargs)
        logfunc('{}: took {:.2} secs.'.format(func.__name__, time() - start))
        return res

    return wrapped


def instrumented(logfunc):
    return partial(log_instrumented, logfunc=logfunc)
