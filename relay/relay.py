"""
    ``You miss 100% of the shots you don't take.''
                       -- Wayne Gretzky
                          -- Michael Scott
"""
import asyncio
from asyncio import Queue, StreamReader, StreamWriter

from . import gcp

import logging
from time import time
from typing import (
    Any, Callable, Coroutine, Dict, Generator, List, NamedTuple, NewType, Set,
    Tuple, Type, Union
)

# Metric values are either integer or float (for now).
Number = Union[int, float]

# Track connections: need a start point for measuring counter-based time series.
CONNECTIONS: Dict[str, Number] = {}

# History of observed metrics and when first seen. { label: { key, time} }
METRICS: Dict[str, Dict[str, Number]] = {}

# Background tasks. Prevent garbage collection.
TASKS: Set[asyncio.Task[None]] = set()


class Metric(NamedTuple):
    label: str
    key: str
    value: Number
    seen: Number

    def guessMetricKind(self) -> gcp.MetricKind:
        if self.key.endswith("count"):
            return gcp.MetricKind.COUNTER
        else:
            return gcp.MetricKind.GAUGE

    def guessValueType(self) -> gcp.MetricType:
        if type(self.value) is int:
            return gcp.MetricType.INT
        elif type(self.value) is float:
            return gcp.MetricType.FLOAT
        else:
            return gcp.MetricType.UNDEFINED

_BAD_DATA = Metric("", "", -1, -1)


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


def background(coroutine: Coroutine[Any, Any, None], name: str) -> None:
    """
    Schedule a coroutine to run in the background.
    """
    task = asyncio.create_task(coroutine, name=name)
    TASKS.add(task)
    task.add_done_callback(TASKS.discard)


def parse(data: bytes) -> Generator[Metric, None, None]:
    """
    Take a given series of bytes and turn into a series of Metrics.
    """
    try:
        for line in data.decode("utf8").splitlines():
            try:
                (raw, val_raw, ts_raw) = line.strip().split(" ")
                pos = raw.find(".")
                label = raw[:pos]
                key = raw[pos+1:]
                value = safe_convert(val_raw)
                seen = safe_convert(ts_raw)
                yield Metric(label, key, value, seen)
            except Exception as e:
                # XXX drop garbage lines for now
                logging.warning(f"dropping line: {e}")
    except Exception as e:
        logging.warning(f"unhandled parser error: {e}")
        yield _BAD_DATA


async def convert_task(q_in: Queue[bytes], q_out: Queue[Metric]) -> None:
    """
    Take raw inbound data and parse into metrics, outputting to q_out.
    """
    while True:
        data = await q_in.get()
        for metric in parse(data):
            # filter out bad data and database-specific metrics (for now)
            if metric is not _BAD_DATA:
                if metric.key.startswith("database"):
                    # skip for now
                    pass
                elif metric.guessMetricKind() == gcp.MetricKind.COUNTER:
                    # skip for now...needs a "start" time
                    pass
                else:
                    logging.debug(f"adding metric {metric}")
                    await q_out.put(metric)
            else:
                logging.warning("conversion error")
        q_in.task_done()


async def shipit(metrics: List[Metric]) -> None:
    """
    Send the metrics to...GCP?
    """
    series = []

    for metric in metrics:
        if not metric.label in METRICS \
           or not metric.key in METRICS[metric.label]:
            # never seen this combo before!
            label = gcp.MetricLabel("neo4j_label",
                                    description="Data from a Neo4j instance.")
            kind = metric.guessMetricKind()
            value_type = metric.guessValueType()

            logging.info(f"creating new metric: {metric})")
            desc = await gcp.create_metric_descriptor(metric.key, kind,
                                                      value_type,
                                                      labels=[label])
            logging.info(f"created descriptor: {desc}")

            # update our history
            old = METRICS.get(metric.label, {})
            old[metric.key] = metric.seen
            METRICS[metric.label] = old

        # convert into a time series
        time_series = gcp.create_time_series(
            metric.key, metric.value, metric.seen, metric.guessValueType(),
            labels={"neo4j_label": metric.label}
        )
        series.append(time_series)

    logging.info(f"writing time series ({len(series)} data points)")
    await gcp.write_time_series(series)


async def publish_task(q: Queue[Metric],
                       shipper: Callable[[List[Metric]], Coroutine[Any, Any, None]],
                       flush_interval: int = 100,
                       flush_timeout: float = 15.0) -> None:
    """
    Take metrics and send them...somewhere!
    """
    batch: List[Any] = []

    # TODO: this is the pluggable part. Parameterize.
    async def publish() -> None:
        """Take our batch and ship it."""
        logging.info(f"flushing {len(batch):,} events")
        background(shipper(batch.copy()), "publish-to-gcp")
        batch.clear()

    async def consume() -> None:
        """Pull a work item off the queue and batch it."""
        event = await q.get()
        batch.append(event)
        q.task_done()

    while True:
        try:
            await asyncio.wait_for(consume(), timeout=flush_timeout)
            if len(batch) >= flush_interval:
                await publish()
        except asyncio.TimeoutError:
            if batch:
                await publish()
        except Exception as e:
            logging.warning(f"unhandled publishing exception: {e}")


def create_consumer(q: Queue[bytes]) \
        -> Callable[[StreamReader, StreamWriter], Coroutine[Any, Any, None]]:
    """
    Capture a given asyncio Queue instance and return a new coroutine that
    consumes from said queue.
    """
    async def consumer(reader: StreamReader, writer: StreamWriter) -> None:
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


async def main(host: str = "127.0.0.1", port: int = 2003) -> None:
    """
    Make rocket go now.
    """
    # Two buckets:
    convert_q: Queue[bytes] = Queue()
    publish_q: Queue[Metric] = Queue()

    # The plumbing.
    consumer = create_consumer(convert_q)
    converter = asyncio.create_task(convert_task(convert_q, publish_q),
                                    name="converter")
    publisher = asyncio.create_task(publish_task(publish_q, shipit),
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
