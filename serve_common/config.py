from threading import RLock
import re
from loguru import logger
# tomli became tomllib of stdlib in 3.11 (PEP 680)
try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

from serve_common.reloading import Reloader
from serve_common import callback
from serve_common.callback import synchronized


CONFIG_FILE = "config.toml"


@logger.catch
def load_config() -> dict:
    try:
        with open(CONFIG_FILE, "rb") as config_file:
            return tomllib.load(config_file)
    except OSError as e:
        logger.warning(f"failed to read config file: {e.strerror}")
    except tomllib.TOMLDecodeError as e:
        logger.error(f"bad TOML in config file: {str(e)}")


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


def find_diff(a: dict, b: dict):
    a_keys = set(a.keys())
    b_keys = set(b.keys())

    common = a_keys & b_keys
    difference = (a_keys | b_keys) - common

    for k in difference:
        yield (k, a.get(k), b.get(k))

    for k in common:
        if isinstance(a[k], dict) and isinstance(b[k], dict):
            for sk, sa, sb in find_diff(a[k], b[k]):
                yield (f"{k}.{sk}", sa, sb)
        elif a[k] != b[k]:
            yield (k, a[k], b[k])


_config_lock = RLock()

class Config:
    def __init__(self):
        self._config_dict = load_config()
        self._reloader = Reloader(CONFIG_FILE, self.reload_config)

        def on_config_change(event, old, new):
            path = event.removeprefix("config:")
            self._set_item(path, new)
        callback.register(re.compile("config:.*"), on_config_change)

    def start_reloader(self):
        self._reloader.start()

    @synchronized(_config_lock)
    def __getitem__(self, path: str):
        return lookup(self._config_dict, path)

    @synchronized(_config_lock)
    def get(self, path: str, default=None):
        return try_lookup(self._config_dict, path, default)

    @synchronized(_config_lock)
    def _set_item(self, path: str, value):
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

        old = parent.get(tail)
        parent[tail] = value
        return old

    def __setitem__(self, path: str, value):
        old = self._set_item(path, value)
        if old != value:
            self._notify_change(path, old, value)

    def _notify_change(self, path, old, new):
        callback.notify(f"config:{path}", (old, new))

    def reload_config(self):
        logger.info("reloading config")
        new_config = load_config()

        with _config_lock:
            old_config = self._config_dict
            self._config_dict = new_config

            for path, old, new in find_diff(old_config, new_config):
                logger.info(f"new config: {path} ({old} -> {new})")
                self._notify_change(path, old, new)


config = Config()
