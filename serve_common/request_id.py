from uuid import uuid4
import threading


context = threading.local()  # gevent also monkeypatches this
def get():
    return getattr(context, "request_id", None)
def set(id):
    context.request_id = id


class RequestTracer:
    """Falcon middleware to add an id to each request for tracing it in the logs."""

    def process_request(self, req, resp):
        context.request_id = str(uuid4())

    def process_response(self, req, resp, res, req_succeeded):
        context.request_id = None
