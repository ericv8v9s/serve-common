from multiprocessing.managers import SyncManager
from threading import RLock

from . import reloading
from .. import callback
from ..callback import synchronized


def lookup(d: dict, path: str):
    keys = path.split(".")
    for key in keys:
        try:
            d = d[key]
        except TypeError as e:
            raise KeyError from e
    return d

def try_lookup(d: dict, path: str, default=None):
    try:
        return lookup(d, path)
    except KeyError:
        return default


_config_lock = RLock()

class _Config:
    def __init__(self):
        self._config_dict = reloading.load_config()
        self._reloader = reloading.Reloader(self.reload_config)
        self._reloader.start()

    @synchronized(_config_lock)
    def __getitem__(self, path: str):
        return lookup(self._config_dict, path)

    @synchronized(_config_lock)
    def get(self, path: str, default=None):
        return try_lookup(self._config_dict, path, default)

    @synchronized(_config_lock)
    def __setitem__(self, path: str, value):
        keys = path.split(".")
        it = iter(keys[:-1])
        tail = keys[-1]
        parent = self._config_dict

        # traverse the part of path that exists
        for key in it:
            if key not in parent or not isinstance(parent[key], dict):
                parent[key] = {}
                parent = parent[key]
                break
            parent = parent[key]

        # build the part of path that does not exist
        for key in it:
            parent[key] = {}
            parent = parent[key]

        old = parent[tail] if tail in parent else None
        new = value
        parent[tail] = value

        self._notify_change(path, old, new)

    def _notify_change(self, path, old, new):
        callback.notify(path, (old, new))

    def reload_config(self):
        new_config = reloading.load_config()
        with _config_lock:
            old_config = self._config_dict
            self._config_dict = new_config
            for path in callback.registered_events():
                old = try_lookup(old_config, path)
                new = try_lookup(new_config, path)
                if old != new:
                    self._notify_change(path, old, new)


# module level singleton instance, initialized in and by manager process
_config_instance = None
def _init_config_instance():
    global _config_instance
    _config_instance = _Config()

class _ConfigManager(SyncManager):
    pass
_ConfigManager.register("Config", lambda: _config_instance, exposed=(
    "__getitem__", "get", "__setitem__", "reload_config"))

_manager = _ConfigManager()
_manager.start(initializer=_init_config_instance)
callback.init_shared_storage(_manager)


config = _manager.Config()
