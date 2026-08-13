import unittest

import joblib
import numpy as np
from distributed import Client

from dask_helper._test_utils import worker
from dask_helper.dask_shared_args import SharedArg


class DaskSharedArgsTest(unittest.TestCase):

    def test_loky_shared_args(self):
        """SharedArg with loky backend passes raw object through."""
        big_data = list(range(1000000))
        with joblib.parallel_backend("loky", n_jobs=-1):
            shared = SharedArg(big_data)
            prepared = shared.prepare()

            self.assertIs(prepared, big_data)
            self.assertIsNone(shared.ref)

            results = joblib.Parallel()(
                joblib.delayed(worker)(shared, x)
                for x in range(10)
            )

            expected_results = [sum(big_data) + x for x in range(10)]
            self.assertEqual(results, expected_results)

    def test_dask_shared_args_prepare_resolve(self):
        """SharedArg with dask backend scatters data and gathers it back."""
        client = Client(processes=False)
        try:
            big_data = list(range(100))
            with joblib.parallel_backend("dask", n_jobs=-1):
                shared = SharedArg(big_data)
                prepared = shared.prepare()

                # prepare() should have set ref
                self.assertIsNotNone(shared.ref)
                # prepare() returns the ref (list of Futures)
                self.assertIs(prepared, shared.ref)

                # Calling prepare() again should reuse the same ref
                prepared2 = shared.prepare()
                self.assertIs(prepared2, prepared)

                # resolve() should gather the data back
                resolved = shared.resolve()
                self.assertEqual(resolved, big_data)
        finally:
            client.close()

    def test_dask_shared_args_resolve_none(self):
        """SharedArg.resolve() returns None when obj is None."""
        shared = SharedArg(None)
        self.assertIsNone(shared.resolve())

    def test_dask_shared_args_resolve_no_ref(self):
        """SharedArg.resolve() returns obj when ref is not set."""
        data = [1, 2, 3]
        shared = SharedArg(data)
        self.assertIs(shared.resolve(), data)

    def test_dask_shared_args_big_data_np(self):
        """SharedArg with dask backend handles large data via scatter+replicate."""
        client = Client(n_workers=2, threads_per_worker=1)
        try:
            big_data = np.arange(1_000_000)
            with joblib.parallel_backend("dask", n_jobs=-1):
                shared = SharedArg(big_data)
                prepared = shared.prepare()

                self.assertIsNotNone(shared.ref)
                self.assertIs(prepared, shared.ref)

                resolved = shared.resolve()
                np.testing.assert_array_equal(resolved, big_data)
        finally:
            client.close()

    def test_dask_shared_args_big_data_list(self):
        """SharedArg with dask backend handles large data via scatter+replicate."""
        client = Client(n_workers=2, threads_per_worker=1)
        try:
            big_data = [list(range(1000000)) for _ in range(10)]
            with joblib.parallel_backend("dask", n_jobs=-1):
                shared = SharedArg(big_data)
                prepared = shared.prepare()

                self.assertIsNotNone(shared.ref)
                self.assertIs(prepared, shared.ref)

                resolved = shared.resolve()
                self.assertEqual(resolved, big_data)
        finally:
            client.close()


    def test_dask_shared_args_in_workers(self):
        """SharedArg with dask backend works in parallel workers via loky fallback."""
        client = Client(processes=False)
        try:
            big_data = list(range(1000000))
            # Use loky backend with dask client available — workers get raw obj
            with joblib.parallel_backend("loky", n_jobs=2):
                shared = SharedArg(big_data)
                shared.prepare()

                results = joblib.Parallel()(
                    joblib.delayed(worker)(shared, x)
                    for x in range(10)
                )

                expected_results = [sum(big_data) + x for x in range(10)]
                self.assertEqual(results, expected_results)
        finally:
            client.close()
