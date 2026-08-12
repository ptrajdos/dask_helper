"""Utility functions used by tests that run inside Ray workers.

These live in the main package so Ray workers can always import them,
regardless of how the test runner discovers and names test modules.
"""

from log_keeper.log_keeper import LogKeeper


def compute(name, queue):
    logger = LogKeeper.get_client_logger(logging_queue=queue, logger_name=f"JLL_{name}")
    logger.debug(f"GG?: {name}")
    LogKeeper.shutdown_client_logger(logger)
    return name

def worker(shared, x):
    shared = shared.resolve()
    return sum(shared) + x