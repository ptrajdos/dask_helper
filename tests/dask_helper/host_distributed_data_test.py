import unittest

import joblib
import numpy as np
from distributed import Client

from dask_helper._test_utils import worker
from dask_helper.host_distributed_data import HostDistributedData


class HostDistributedDataTest(unittest.TestCase):

    def test_data_distribution(self):
        """SharedArg with loky backend passes raw object through."""
        big_data = list(range(10000))
        backends = ["loky", "dask"]
        client = Client(n_workers=2, threads_per_worker=1)
        for backend in backends:
            with self.subTest(backend=backend):
                with joblib.parallel_backend(backend, n_jobs=-1):
                    shared = HostDistributedData(data=big_data, name="big_data")
                    future = shared.prepare(backend=backend)
                    
                    results = joblib.Parallel()(
                        joblib.delayed(worker)(future, x)
                        for x in range(10)
                    )

                    expected_results = [sum(big_data) + x for x in range(10)]
                    self.assertEqual(results, expected_results)

    # def test_dask_shared_args_big_data_np(self):
    #     """SharedArg with dask backend handles large data via scatter+replicate."""
    #     client = Client(n_workers=2, threads_per_worker=1)
    #     try:
    #         big_data = np.arange(1_000_000)
    #         with joblib.parallel_backend("dask", n_jobs=-1):
    #             shared = SharedArg(big_data)
    #             prepared = shared.prepare()

    #             self.assertIsNotNone(shared.ref)
    #             self.assertIs(prepared, shared.ref)

    #             resolved = shared.resolve()
    #             np.testing.assert_array_equal(resolved, big_data)
    #     finally:
    #         client.close()

    # def test_dask_shared_args_big_data_list(self):
    #     """SharedArg with dask backend handles large data via scatter+replicate."""
    #     client = Client(n_workers=2, threads_per_worker=1)
    #     try:
    #         big_data = [list(range(1000000)) for _ in range(10)]
    #         with joblib.parallel_backend("dask", n_jobs=-1):
    #             shared = SharedArg(big_data)
    #             prepared = shared.prepare()

    #             self.assertIsNotNone(shared.ref)
    #             self.assertIs(prepared, shared.ref)

    #             resolved = shared.resolve()
    #             self.assertEqual(resolved, big_data)
    #     finally:
    #         client.close()


    # def test_dask_shared_args_in_workers(self):
    #     """SharedArg with dask backend works in parallel workers via loky fallback."""
    #     client = Client(processes=False)
    #     try:
    #         big_data = list(range(1000000))
    #         # Use loky backend with dask client available — workers get raw obj
    #         with joblib.parallel_backend("loky", n_jobs=2):
    #             shared = SharedArg(big_data)
    #             shared.prepare()

    #             results = joblib.Parallel()(
    #                 joblib.delayed(worker)(shared, x)
    #                 for x in range(10)
    #             )

    #             expected_results = [sum(big_data) + x for x in range(10)]
    #             self.assertEqual(results, expected_results)
    #     finally:
    #         client.close()
