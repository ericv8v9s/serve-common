import os
import time
import threading


class _AbstractReloader(threading.Thread):
    def __init__(self, filename: str, reload_func):
        super().__init__()
        self.daemon = True
        self.filename = filename
        self.reload_func = reload_func


class PollingReloader(_AbstractReloader):
    def __init__(self, filename, reload_func):
        super().__init__(filename, reload_func)

    def run(self):
        old_mtime = mtime = None
        while True:
            time.sleep(1)
            try:
                mtime = os.stat(self.filename).st_mtime
            except OSError:
                pass

            if mtime is None:
                continue
            if old_mtime is not None and old_mtime < mtime:
                try:
                    self.reload_func()
                except:
                    import traceback
                    traceback.print_exc()
            old_mtime = mtime


# We might detect and conditionally provide better implementations.
Reloader = PollingReloader
