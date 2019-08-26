from posix.time cimport clock_gettime, timespec, CLOCK_REALTIME

cpdef busy():
    """
    This is the worst kind of busy loop - never do this as a wait method.
    """
    cdef timespec ts
    clock_gettime(CLOCK_REALTIME, &ts)
    cdef double initial = ts.tv_sec + (ts.tv_nsec / 1_000_000_000.)
    cdef double elapsed = 0.0

    while elapsed < 2:
        clock_gettime(CLOCK_REALTIME, &ts)
        elapsed = ts.tv_sec + (ts.tv_nsec / 1_000_000_000.) - initial

        # Adding any python call such as `print` here will allow the python signal handler run and raise
        # https://docs.python.org/3/library/signal.html#execution-of-python-signal-handlers
