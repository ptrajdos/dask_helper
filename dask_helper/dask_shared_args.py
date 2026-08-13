import pickle

from joblib.parallel import get_active_backend


class SharedArg:
    def __init__(self, obj):
        self.obj = obj
        self.ref = None
        self._resolved = None
        self._pre_serialized = False

    def prepare(self):
        backend, _ = get_active_backend()

        backend_name = backend.__class__.__name__.lower()

        if "dask" in backend_name:
            if self.ref is None:
                from distributed import get_client
                client = get_client()
                data = self.obj
                try:
                    import numpy as np
                    is_efficient = isinstance(data, np.ndarray)
                except ImportError:
                    is_efficient = False
                if not is_efficient:
                    data = pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL)
                    self._pre_serialized = True
                future = client.scatter(data, hash=False)
                client.replicate(future)
                self.ref = future
            return self.ref

        return self.obj

    def resolve(self):
        if isinstance(self.obj, type(None)):
            return None

        if self.ref is not None:
            if self._resolved is None:
                from distributed import get_client
                client = get_client()
                result = client.gather(self.ref)
                if self._pre_serialized:
                    result = pickle.loads(result)
                self._resolved = result
            return self._resolved

        return self.obj
