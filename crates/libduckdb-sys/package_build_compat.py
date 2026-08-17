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


def relative_package_path(path, package_root):
    """Normalize a package path and remove its absolute package root."""
    normalized_path = path.replace("\\", "/")
    normalized_root = str(package_root).replace("\\", "/").rstrip("/")
    return normalized_path.removeprefix(f"{normalized_root}/")
