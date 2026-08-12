import logging
from log_keeper.log_keeper import LogKeeper


class _DaskQueueWrapper:
    """Wraps a ``distributed.Queue`` to provide the ``put_nowait`` method
    expected by Python's ``logging.handlers.QueueHandler``.

    Pickling delegates to the underlying ``distributed.Queue`` so that
    Dask workers can receive this object without serialisation issues.
    """

    def __init__(self, dask_queue):
        self._queue = dask_queue

    def put_nowait(self, item):
        self._queue.put(item)

    def put(self, item, **kwargs):
        self._queue.put(item, **kwargs)

    def get(self, **kwargs):
        return self._queue.get(**kwargs)

    def qsize(self):
        return self._queue.qsize()

    def __getattr__(self, name):
        return getattr(self._queue, name)

    def __reduce__(self):
        return (_DaskQueueWrapper, (self._queue,))


def _dask_queue_forwarder(dask_queue, logkeeper_queue, stop_event):
    """Background thread that drains a ``distributed.Queue`` into the
    LogKeeper ``multiprocessing.Manager().Queue()``.

    Runs until *stop_event* is set **and** the dask_queue is empty.

    Uses ``qsize()`` polling instead of ``get(timeout=...)`` to avoid
    server-side ``TimeoutError`` noise from the distributed scheduler.
    """
    import time

    while not stop_event.is_set():
        try:
            if dask_queue.qsize() == 0:
                time.sleep(0.1)
                continue
            record = dask_queue.get()
            logkeeper_queue.put(record)
        except Exception:
            time.sleep(0.1)
            continue
    # Drain remaining records after stop signal
    while True:
        try:
            if dask_queue.qsize() == 0:
                break
            record = dask_queue.get()
            logkeeper_queue.put(record)
        except Exception:
            break


class DaskLogKeeperHelper:
    """Helper for Dask-compatible LogKeeper logging.

    When using Dask as a joblib backend via ``distributed``, workers
    cannot receive the multiprocessing Manager queue because it cannot
    be serialized.  This helper creates a ``distributed.Queue`` for
    worker processes and forwards records into the real LogKeeper queue.
    """

    def __init__(self, logging_queue, backend="loky"):
        self.logging_queue = logging_queue
        self.backend = backend
        self.worker_logging_queue = logging_queue
        self._dask_forwarder_stop = None
        self._dask_forwarder_thread = None

        if self.backend == "dask":
            from distributed import Queue as DaskQueue
            import threading

            import uuid
            self.worker_logging_queue = _DaskQueueWrapper(DaskQueue(name=f"logkeeper_queue_{uuid.uuid4().hex}"))
            self._dask_forwarder_stop = threading.Event()
            self._dask_forwarder_thread = threading.Thread(
                target=_dask_queue_forwarder,
                args=(
                    self.worker_logging_queue,
                    self.logging_queue,
                    self._dask_forwarder_stop,
                ),
                daemon=True,
            )

    def start(self):
        if self._dask_forwarder_thread is not None:
            self._dask_forwarder_thread.start()

    def quit(self):
        if self._dask_forwarder_stop is not None:
            self._dask_forwarder_stop.set()
        if self._dask_forwarder_thread is not None:
            self._dask_forwarder_thread.join(timeout=5.0)

    def get_worker_queue(self):
        return self.worker_logging_queue

    @staticmethod
    def get_logger(logging_queue, name):
        """Return a LogKeeper client logger if queue is available, else a standard logger."""
        if logging_queue is not None:
            return LogKeeper.get_client_logger(logging_queue=logging_queue, logger_name=name)
        return logging.getLogger(name)

    @staticmethod
    def shutdown_logger(logging_queue, logger):
        """Shutdown LogKeeper client logger only when queue is available."""
        if logging_queue is not None:
            LogKeeper.shutdown_client_logger(logger)
