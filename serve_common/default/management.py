import falcon
import spectree
from pydantic import BaseModel as Schema
from pydantic import Field
from loguru import logger

from serve_common.spec import spec
from serve_common.config import config


version = "0.0.1"


def route(app: falcon.App):
    logger.info("initializing management endpoints")
    openapi_spec = OpenAPISpec()
    app.add_route("/", openapi_spec)
    app.add_route("/manage/spec", openapi_spec)

    ver_res = Version()
    app.add_route("/manage/version", ver_res)

    log_level_config = LogLevel()
    app.add_route("/manage/logging/level", log_level_config)


management_tag = spectree.Tag(
    name="management",
    description="Miscellaneous management operations."
)


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


class Version:
    @logger.catch(reraise=True)
    @spec.validate(tags=[management_tag])
    def on_get(self, req, resp):
        """Returns the version string."""
        resp.text = version


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
