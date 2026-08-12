from multiprocessing import freeze_support
import time
import socket
from collections import Counter

from distributed import Client
from joblib import Parallel, delayed, parallel_config



def work(i):
    time.sleep(5)  # make tasks visible in htop
    return {
        "task": i,
        "host": socket.gethostname(),
        "worker": __import__("distributed").get_worker().address,
        "pid": __import__("os").getpid(),
    }


def main():
    freeze_support()
    # Connect to existing Dask cluster
    try:
        client = Client(address="tcp://127.0.0.1:8786")
    except Exception as e:
        print(f"Failed to connect to Dask cluster: {e}")
        print("Starting a new local Dask cluster...")
        client = Client()
    print("Dask cluster info:")
    print(client.scheduler_info())
    with parallel_config(backend="dask", n_jobs=-1):
        results = Parallel(verbose=10)(
            delayed(work)(i) for i in range(20)
        )


    print("\nDistribution:")
    print(Counter(r["host"] for r in results))

    print("\nDetails:")
    for r in results:
        print(r)

    client.close()


if __name__ == "__main__":
    main()