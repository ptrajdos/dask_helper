from joblib.parallel import get_active_backend


class SharedArg:
    def __init__(self, obj):
        self.obj = obj
        self.ref = None

    def prepare(self):
        backend, _ = get_active_backend()

        backend_name = backend.__class__.__name__.lower()

        if "dask" in backend_name:
            if self.ref is None:
                from distributed import get_client
                client = get_client()
                self.ref = client.scatter(self.obj, broadcast=True)
            return self.ref

        return self.obj

    def resolve(self):
        if isinstance(self.obj, type(None)):
            return None

        if self.ref is not None:
            from distributed import get_client
            client = get_client()
            return client.gather(self.ref)

        return self.obj
