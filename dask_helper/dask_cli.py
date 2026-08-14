"""CLI entry points for managing Dask scheduler/worker nodes."""

import argparse
import os
import subprocess
import sys


def _gb_to_bytes(gb):
    return gb * 1024 * 1024 * 1024


def _dask_cmd():
    """Return the path to the ``dask`` CLI executable."""
    return os.path.join(os.path.dirname(sys.executable), "dask")


def dask_scheduler():
    parser = argparse.ArgumentParser(description="Start a Dask scheduler")
    parser.add_argument("--ip", default="127.0.0.1", help="Scheduler IP address (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8786, help="Scheduler port (default: 8786)")
    parser.add_argument("--dashboard-port", type=int, default=8787, help="Dashboard port (default: 8787)")
    parser.add_argument("--session-token-expiration", type=int, default=2678400, help="Token expiration time in seconds (default: 3600)")

    args = parser.parse_args()

    sys.exit(subprocess.run([
        _dask_cmd(), "scheduler",
        f"--host={args.ip}",
        f"--port={args.port}",
        f"--dashboard-address=:{args.dashboard_port}",
        f"--session-token-expiration={args.token_expiration}",
    ]).returncode)


def dask_worker():
    parser = argparse.ArgumentParser(description="Start a Dask worker node")
    parser.add_argument("--ip", default="127.0.0.1", help="Scheduler IP address (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8786, help="Scheduler port (default: 8786)")
    parser.add_argument("--num-cpus", type=int, default=4, help="Number of CPUs (nthreads) (default: 4)")
    parser.add_argument("--memory-gb", type=int, default=10, help="Memory limit in GB (default: 10)")
    args = parser.parse_args()

    sys.exit(subprocess.run([
        _dask_cmd(), "worker",
        f"tcp://{args.ip}:{args.port}",
        f"--nthreads={args.num_cpus}",
        f"--memory-limit={_gb_to_bytes(args.memory_gb)}",
    ]).returncode)


def dask_stop():
    """Stop all Dask scheduler and worker processes."""
    import signal

    stopped = False
    for proc_name in ["dask-scheduler", "dask-worker", "dask scheduler", "dask worker"]:
        result = subprocess.run(
            ["pkill", "-f", proc_name],
            capture_output=True,
        )
        if result.returncode == 0:
            stopped = True

    if stopped:
        print("Stopped Dask processes.")
    else:
        print("No Dask processes found.")
    sys.exit(0)
