class RecursiveMkdirOS:
    """Proxy an OS module while making its mkdir calls recursive."""

    def __init__(self, delegate):
        self._delegate = delegate

    def __getattr__(self, name):
        return getattr(self._delegate, name)

    def mkdir(self, path, mode=0o777, *, dir_fd=None):
        if dir_fd is not None:
            return self._delegate.mkdir(path, mode, dir_fd=dir_fd)
        self._delegate.makedirs(path, mode=mode, exist_ok=True)
