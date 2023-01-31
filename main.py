#!/usr/bin/env python3
import logging
from time import gmtime


class UTCFormatter(logging.Formatter):
    """
    https://docs.python.org/3/howto/logging-cookbook.html#formatting-times-using-utc-gmt-via-configuration
    """
    converter = gmtime


# Mimic Neo4j logging style as best as possible
LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "utc": {
            "()": UTCFormatter,
            "format": "%(asctime)s %(levelname)-8s [%(name)-12s] %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S%z",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "utc",
        },
    },
    "root": { "handlers": [ "console" ], "level": "DEBUG" },
}


if __name__ == "__main__":
    import asyncio
    import relay
    import logging.config
    try:
        logging.config.dictConfig(LOGGING_CONFIG)
        asyncio.run(relay.main())
    except KeyboardInterrupt:
        # XXX ctrl-c caught...we should flush
        pass
