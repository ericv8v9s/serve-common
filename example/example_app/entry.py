from serve_common import ipc, callback
from serve_common.config import config

import serve_common.default.logging as default_logging
import serve_common.default.falcon as default_falcon
import serve_common.default.management as management


def create_app():
    default_logging.add_output_stderr()
    default_logging.add_output_file()

    callback.enable_ipc()
    callback.start_callback_loop()

    config.start_reloader()

    return default_falcon.setup_falcon(custom_routes=[
            management.route,
            # add application routes here
    ])
