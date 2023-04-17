import falcon
from loguru import logger

from serve_common.management import *


def route_management(app: falcon.App):
    logger.info("initializing management endpoints")
    openapi_spec = OpenAPISpec()
    app.add_route("/", openapi_spec)
    app.add_route("/manage/spec", openapi_spec)

    version_hash = VersionHash()
    app.add_route("/manage/hash", version_hash)

    log_level_config = LogLevel()
    app.add_route("/manage/logging/level", log_level_config)

