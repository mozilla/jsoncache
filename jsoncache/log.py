# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.


import os
import logging.config

_LOG_CONFIG = {
    # Note that the formatters.json.logger_name must match
    # loggers.<logger_name> key
    "version": 1,
    "formatters": {
        "json": {"()": "dockerflow.logging.JsonLogFormatter", "logger_name": "srg",}
    },
    "handlers": {
        "console": {
            "level": "INFO",
            "class": "logging.StreamHandler",
            "formatter": "json",
        },
    },
    "loggers": {"srg": {"handlers": ["console"], "level": "INFO",},},
}


def get_logger(name=None):
    LOG_LEVEL = int(os.environ.get("LOG_LEVEL", logging.INFO))

    _LOG_CONFIG["loggers"]["srg"]["level"] = LOG_LEVEL
    logging.config.dictConfig(_LOG_CONFIG)
    log = logging.getLogger(f"srg.{name}")
    log.setLevel(LOG_LEVEL)
    return log
