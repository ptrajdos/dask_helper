from __future__ import annotations

from collections import defaultdict
from typing import Any

from dask.distributed import Client, Future, get_client


class HostDistributedData:
    """
    Manage a large shared object for local Joblib backends and Dask.

    Dask:
        - creates one copy of the data per host;
        - does not broadcast to every worker;
        - returns a Dask Future.

    Other backends (e.g. loky):
        - does not use Dask;
        - returns the original object unchanged.

    If workers/hosts are added later, call refresh() to ensure the
    new hosts also receive a replica.
    """

    def __init__(
        self,
        data: Any,
        *,
        client: Client | None = None,
        name: str | None = None,
        hash: bool = False,
    ):
        self.data = data
        self.client = client
        self.name = name
        self.hash = hash

        self._future: Future | None = None
        self._dask_enabled = False

    # ------------------------------------------------------------------
    # Client
    # ------------------------------------------------------------------

    def _get_client(self) -> Client:
        """
        Return the explicitly supplied client or discover the currently
        active Dask client.
        """
        if self.client is not None:
            return self.client

        return get_client()

    # ------------------------------------------------------------------
    # Worker / host discovery
    # ------------------------------------------------------------------

    def _workers_by_host(
        self,
        client: Client,
    ) -> dict[str, list[str]]:
        """
        Return worker addresses grouped by host.

        Example:

            {
                "192.168.192.10": [
                    "tcp://192.168.192.10:40001",
                    ...
                ],
                "192.168.192.90": [
                    "tcp://192.168.192.90:46051",
                    ...
                ],
            }
        """
        workers = client.scheduler_info()["workers"]

        result: dict[str, list[str]] = defaultdict(list)

        for address, info in workers.items():
            result[info["host"]].append(address)

        return dict(result)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def prepare(self, backend: str) -> Any:
        """
        Prepare the data for a Joblib backend.

        Parameters
        ----------
        backend:
            Joblib backend name, e.g. "dask", "loky", "threading".

        Returns
        -------
        Dask Future for the Dask backend.

        Original data for all other backends.
        """
        if backend.lower() != "dask":
            self._dask_enabled = False
            return self.data

        self._dask_enabled = True

        client = self._get_client()

        if self._future is None:
            self._future = self._scatter_to_hosts(client)

        return self._future

    def refresh(self) -> Any:
        """
        Ensure that every currently available host has a replica.

        Useful after new workers/hosts have been added to the cluster.

        For non-Dask backends this simply returns the original object.
        """
        if not self._dask_enabled:
            return self.data

        client = self._get_client()

        if self._future is None:
            raise RuntimeError(
                "prepare('dask') must be called before refresh()"
            )

        workers_by_host = self._workers_by_host(client)

        if not workers_by_host:
            raise RuntimeError("Dask scheduler has no workers")

        representatives = [
            workers[0]
            for workers in workers_by_host.values()
        ]

        client.replicate(
            [self._future],
            workers=representatives,
        )

        return self._future

    # ------------------------------------------------------------------
    # Dask implementation
    # ------------------------------------------------------------------

    def _scatter_to_hosts(
        self,
        client: Client,
    ) -> Future:
        """
        Create one copy of the data on each currently available host.
        """
        workers_by_host = self._workers_by_host(client)

        if not workers_by_host:
            raise RuntimeError("Dask scheduler has no workers")

        # Pick one worker from each physical host.
        representatives = [
            workers[0]
            for workers in workers_by_host.values()
        ]

        # Create the initial copy on one worker.
        future = client.scatter(
            self.data,
            workers=[representatives[0]],
            broadcast=False,
            hash=self.hash,
        )

        # Replicate the Future to one worker on every host.
        client.replicate(
            [future],
            workers=representatives,
        )

        return future

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    def owners(self) -> dict[str, list[str]]:
        """
        Return Dask worker addresses currently holding the data.
        """
        if self._future is None:
            return {}

        client = self._get_client()

        return client.who_has(self._future)

    def host_owners(self) -> dict[str, list[str]]:
        """
        Return hosts currently holding the data.

        Example:

            {
                "192.168.192.10": [
                    "tcp://192.168.192.10:40001"
                ],
                "192.168.192.90": [
                    "tcp://192.168.192.90:46051"
                ],
            }
        """
        if self._future is None:
            return {}

        client = self._get_client()

        workers = client.scheduler_info()["workers"]
        owners = client.who_has(self._future)

        result: dict[str, list[str]] = defaultdict(list)

        for worker in owners.get(self._future.key, []):
            info = workers.get(worker)

            if info is not None:
                result[info["host"]].append(worker)

        return dict(result)

    @property
    def future(self) -> Future | None:
        """Return the underlying Dask Future, if one exists."""
        return self._future