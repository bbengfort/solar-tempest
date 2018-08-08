# iaga.utils
# Utility functions and helpers
#
# Author:  Benjamin Bengfort <benjamin@bengfort.com>
# Created: Wed Aug 08 12:00:44 2018 -0400
#
# ID: utils.py [] benjamin@bengfort.com $

"""
Utility functions and helpers
"""

##########################################################################
## Imports
##########################################################################

import time

from functools import wraps


##########################################################################
## Decorators
##########################################################################

def memoized(fget):
    """
    Return a property attribute for new-style classes that only calls its
    getter on the first access. The result is stored and on subsequent
    accesses is returned, preventing the need to call the getter any more.
    Parameters
    ----------
    fget: function
        The getter method to memoize for subsequent access.
    See also
    --------
    python-memoized-property
        `python-memoized-property <https://github.com/estebistec/python-memoized-property>`_
    """
    attr_name = '_{0}'.format(fget.__name__)

    @wraps(fget)
    def fget_memoized(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, fget(self))
        return getattr(self, attr_name)

    return property(fget_memoized)


##########################################################################
## Timer Class
##########################################################################


def human_readable_time(s):
    h, s = divmod(s, 3600)
    m, s = divmod(s, 60)
    return "{:>02.0f}:{:02.0f}:{:>07.4f}".format(h, m, s)


class Timer:
    """
    A context object timer. Usage:
        >>> with Timer() as timer:
        ...     do_something()
        >>> print timer.interval
    """
    def __init__(self):
        self.time = time.time

    def __enter__(self):
        self.start = self.time()
        return self

    def __exit__(self, *exc):
        self.finish = self.time()
        self.interval = self.finish - self.start

    def __str__(self):
        return human_readable_time(self.interval)
