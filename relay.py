#!/usr/bin/env python3
"""
    ``You miss 100% of the shots you don't take.''
                       -- Wayne Gretzky
                          -- Michael Scott
"""
import asyncio
import logging
from time import time, gmtime
from typing import Coroutine, Generator, NewType, Tuple, Union

# Metric values are either integer or float (for now).
Number = NewType('Number', Union[int, float])

# Track connections: need a start point for measuring counter-based time series.
CONNECTIONS = {}

# Special garbage value so we don't have to wrangle None's
BAD_DATA = ("", -1, -1)


def safe_convert(s: str) -> Number:
    """
    Convert a given string to the proper native numeric type (int, float).

    As falsey input, e.g. "", returns 0.
    """
    if not s:
        return 0
    try:
        return int(s)
    except ValueError:
        return float(s)


def parse(data: bytes) -> Generator[Tuple[str, Number, Number], None, None]:
    """
    Take a given series of bytes and turn into a series of Metrics.
    """
    try:
        for line in data.decode("utf8").splitlines():
            try:
                (metric, val_raw, ts_raw) = line.strip().split(" ")
                val = safe_convert(val_raw)
                ts = safe_convert(ts_raw)
                yield (metric, val, ts)
            except Exception as e:
                # XXX drop garbage lines for now
                logging.warning(f"dropping line: {e}")
    except Exception as e:
        logging.warning(f"unhandled parser error: {e}")
        yield BAD_DATA


async def convert_task(q_in: asyncio.Queue, q_out: asyncio.Queue):
    """
    Take raw inbound data and parse into metrics, outputting to q_out.
    """
    while True:
        data = await q_in.get()
        for metric in parse(data):
            if metric is not BAD_DATA:
                logging.info(f"adding metric {metric}")
                await q_out.put(metric)
            else:
                logging.warning("conversion error")
        q_in.task_done()


async def publish_task(q: asyncio.Queue):
    """
    Take metrics and send them...somewhere!
    """
    batch = []

    # TODO: this is the pluggable part. Parameterize.
    async def publish():
        """Take our batch and ship it."""
        logging.info(f"flushing {len(batch):,} events")
        await asyncio.sleep(0.3)
        batch.clear()

    async def consume():
        """Pull a work item off the queue and batch it."""
        event = await q.get()
        logging.info(f"appending {event}")
        batch.append(event)
        q.task_done()

    while True:
        try:
            await asyncio.wait_for(consume(), timeout=15.0)
            if len(batch) >= 33:
                await publish()
        except asyncio.TimeoutError:
            if batch:
                await publish()
        except Exception as e:
            logging.warning(f"unhandled publishing exception: {e}")


def create_consumer(q: asyncio.Queue) -> Coroutine:
    """
    Capture a given asyncio Queue instance and return a new coroutine that
    consumes from said queue.
    """
    async def consumer(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """
        Primary connection handler.
        """
        # Who is it?
        (host, port) = writer.get_extra_info("peername")
        client = f"{host}:{port}"
        logging.debug(f"got connection from {client}")
        CONNECTIONS.update({ client: time() })

        # Chug along until EOF, implying disconnect.
        while not reader.at_eof():
            line = await reader.readline()
            if line:
                await q.put(line)

        # Teardown and cleanup.
        duration = time() - CONNECTIONS.pop(client)
        logging.debug(f"goodbye {client} (duration: {round(duration, 2)}s)!")

    return consumer


async def main(host: str = "127.0.0.1", port: int = 2003):
    """
    Make rocket go now.
    """
    # Two buckets:
    convert_q = asyncio.Queue()
    publish_q = asyncio.Queue()

    # The plumbing.
    consumer = create_consumer(convert_q)
    converter = asyncio.create_task(convert_task(convert_q, publish_q),
                                    name="converter")
    publisher = asyncio.create_task(publish_task(publish_q),
                                    name="publisher")

    # Fire up the server. Let it flow.
    server = await asyncio.start_server(consumer, host=host, port=port)
    async with server:
        for addr in [sock.getsockname() for sock in server.sockets]:
            logging.info(f"listening on {addr[0]}:{addr[1]}")
        await server.serve_forever()

    # Drain any remaining work items.
    # XXX this isn't reached...need to hook into signal handler
    await asyncio.wait((convert_q.join(), publish_q.join()), timeout=30.0)


#############################################################################

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
    import logging.config
    try:
        logging.config.dictConfig(LOGGING_CONFIG)
        asyncio.run(main())
    except KeyboardInterrupt:
        # XXX ctrl-c caught...we should flush
        pass
