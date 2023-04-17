from typing import Callable
from falcon import App
from loguru import logger

from serve_common.spec import spec
from serve_common.request_id import RequestTracer
from serve_common.log_util import RequestTimer
from . import management


def setup_falcon(
        custom_routes: list[Callable[[App], None]]=[management.route]
) -> App:
    logger.info("initializing falcon app")
    app = App(middleware=[RequestTracer(), RequestTimer()])

    for route in custom_routes:
        route(app)

    spec.register(app)

    logger.success("initialization complete")
    return app
