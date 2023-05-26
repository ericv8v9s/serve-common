# refer to https://docs.gunicorn.org/en/stable/settings.html

bind = "0.0.0.0:8000"
# Can be overriden by CLI argument --bind

loglevel = "info"
accesslog = "logs/gunicorn_access.log"
errorlog = "logs/gunicorn_error.log"

preload_app = True
"""Load application code before the worker processes are forked."""
# If using pytorch, pytorch's intra-op parallelism must be set to 1
# to enable preload app.
# It seems pytorch internally has parallelism which, after gunicorn's fork,
# leaves some locks unacquirable, causing the workers to deadlock and timeout
# whenever they try to use the models.

workers = 4
"""
The number of worker processes for handling requests.

A positive integer generally in the 2-4 x $(NUM_CORES) range.
You'll want to vary this a bit to find the best
for your particular application's work load.
"""

worker_class = "sync"
"""https://docs.gunicorn.org/en/stable/design.html"""

threads = 1
"""
The number of worker threads for handling requests.

Run each worker with the specified number of threads.
"""
# Setting this above 1 will override worker_class to "gthread".
