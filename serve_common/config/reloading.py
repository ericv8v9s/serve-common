import sys
import threading

# tomli became tomllib of stdlib in 3.11 (PEP 680)
try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

from loguru import logger


has_inotify = False
#if sys.platform.startswith('linux'):
#    try:
#        from inotify.adapters import Inotify
#        import inotify.constants
#        has_inotify = True
#    except ImportError:
#        pass


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


class AbstractReloader(threading.Thread):
    def __init__(self, reload_func):
        super().__init__()
        self.daemon = True
        self.reload_func = reload_func


# use inotify if we have it, use a polling implementation otherwise
if has_inotify:
    class Reloader(AbstractReloader):
        event_mask = (
                inotify.constants.IN_CREATE |
                inotify.constants.IN_DELETE |
                inotify.constants.IN_CLOSE_WRITE )

        def __init__(self, reload_func):
            super().__init__(reload_func)
            self.watcher = Inotify()

        def run(self):
            self.watcher.add_watch("./", mask=self.event_mask)
            for event in self.watcher.event_gen(yield_nones=False):
                filename = event[3]
                if filename == CONFIG_FILE:
                    self.reload_func()

else:
    import os
    import time

    class Reloader(AbstractReloader):
        def __init__(self, reload_func):
            super().__init__(reload_func)

        def run(self):
            old_mtime = mtime = None
            while True:
                time.sleep(1)
                try:
                    mtime = os.stat(CONFIG_FILE).st_mtime
                except OSError:
                    pass

                if mtime is None:
                    continue
                if old_mtime is None or old_mtime < mtime:
                    self.reload_func()
                old_mtime = mtime
