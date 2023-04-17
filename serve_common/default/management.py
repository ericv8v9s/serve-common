import falcon
import spectree
from pydantic import BaseModel as Schema
from pydantic import Field
from loguru import logger

from serve_common.export import *
from serve_common.spec import spec
from serve_common.config import config
from serve_common import versionhash


def route(app: falcon.App):
    logger.info("initializing management endpoints")
    openapi_spec = OpenAPISpec()
    app.add_route("/", openapi_spec)
    app.add_route("/manage/spec", openapi_spec)

    version_hash = VersionHash()
    app.add_route("/manage/hash", version_hash)

    log_level_config = LogLevel()
    app.add_route("/manage/logging/level", log_level_config)


management_tag = spectree.Tag(
    name="management",
    description="Miscellaneous management operations."
)


@export
class OpenAPISpec:
    class OpenAPISpecJson(Schema):
        """Open API specification for this service in JSON format."""
        pass

    @logger.catch(reraise=True)
    @spec.validate(tags=[management_tag],
        resp=spectree.Response(HTTP_200=OpenAPISpecJson),
        skip_validation=True)
    def on_get(self, req, resp):
        """Generates a Open API specification JSON document."""
        resp.media = spec.spec


@export
class VersionHash:
    class VersionHashResp(Schema):
        sha256: str

    @logger.catch(reraise=True)
    @spec.validate(tags=[management_tag],
        resp=spectree.Response(HTTP_200=VersionHashResp))
    def on_get(self, req, resp):
        """Returns a hash of this program itself."""
        resp.media = { "sha256": versionhash.sha256sum }


@export
class LogLevel:
    class LogLevelBody(Schema):
        level: str = Field(..., description=
        "Current (GET) or desired (POST) log level.")

    @logger.catch(reraise=True)
    @spec.validate(tags=[management_tag],
        resp=spectree.Response(HTTP_200=LogLevelBody))
    def on_get(self, req, resp):
        """Returns the current logging level."""
        resp.media = { "level": config.get("logging.level") }

    @logger.catch(reraise=True)
    @spec.validate(tags=[management_tag],
        json=LogLevelBody)
    def on_post(self, req, resp):
        """Sets a new logging level."""
        level = req.media["level"]
        logger.info(
                f"requested to change log level "
                f"({config.get('logging.level')} => {level})")
        config["logging.level"] = level
