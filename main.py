#!/usr/bin/env python3
import argparse
import asyncio
import logging
import logging.config

from time import gmtime
from typing import Any, Dict, Union

class UTCFormatter(logging.Formatter):
    """
    https://docs.python.org/3/howto/logging-cookbook.html#formatting-times-using-utc-gmt-via-configuration
    """
    converter = gmtime


# Mimic Neo4j logging style as best as possible
LOGGING_CONFIG: Dict[str, Any] = {
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
    "root": { "handlers": [ "console" ], "level": "INFO" },
}


if __name__ == "__main__":
    import relay

    parser = argparse.ArgumentParser(
        prog="metrics-relay",
        description="Sends Neo4j metrics to the Cloud",
        epilog="be kind, rewind")
    parser.add_argument("-d", "--debug", dest="debug", action="store_true",
                        help="turn on debug logging")
    parser.add_argument("--host", dest="host",
                        default="localhost", help="hostname or ip to listen on")
    parser.add_argument("--port", dest="port", type=int,
                        default=2003, help="tcp port to listen on")
    try:
        args = parser.parse_args()

        if args.debug:
            root = LOGGING_CONFIG["root"]
            root.update({"level", "DEBUG"})
            LOGGING_CONFIG.update(root)
        logging.config.dictConfig(LOGGING_CONFIG)

        asyncio.run(relay.main(args.host, args.port))
    except KeyboardInterrupt:
        # XXX ctrl-c caught...we should flush
        pass
