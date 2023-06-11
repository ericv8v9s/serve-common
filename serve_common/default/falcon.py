from typing import Callable
from falcon import App

import logging

from serve_common.spec import spec
from serve_common.request_id import RequestTracer
from serve_common.logging import RequestTimer
from . import management


logger = logging.getLogger(__name__)


def setup_falcon(
        custom_routes: list[Callable[[App], None]]=[management.route]
) -> App:
    logger.info("initializing falcon app")
    app = App(middleware=[RequestTracer(), RequestTimer()])

    for route in custom_routes:
        route(app)

    spec.register(app)

    logger.info("initialization complete")
    return app
