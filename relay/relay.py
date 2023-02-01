"""
    ``You miss 100% of the shots you don't take.''
                       -- Wayne Gretzky
                          -- Michael Scott
"""
import asyncio
from asyncio import Queue, StreamReader, StreamWriter

from . import gcp
from .metric import *

import logging
from time import time
from typing import (
    Any, Callable, Coroutine, Dict, Generator, List, NamedTuple, NewType, Set,
    Tuple, Type, Union
)

# Metric values are either integer or float (for now).
Number = Union[int, float]

# Track historical connections.
#   key: hostname/ip of the system sending us metrics
# value: the time we first saw that host
CONNECTIONS: Dict[str, float] = {}

# Background tasks. Prevent garbage collection.
TASKS: Set[asyncio.Task[None]] = set()


class WorkItem(NamedTuple):
    buffer: bytes
    client: str



def background(coroutine: Coroutine[Any, Any, None], name: str) -> None:
    """
    Schedule a coroutine to run in the background.
    """
    task = asyncio.create_task(coroutine, name=name)
    TASKS.add(task)
    task.add_done_callback(TASKS.discard)


def parse(data: bytes, host: str,
          first_seen: float = 0.0) -> Generator[Neo4j5Metric, None, None]:
    """
    Take a given series of bytes and turn into a series of Neo4jMetrics.
    """
    try:
        for line in data.decode("utf8").splitlines():
            try:
                raw = GraphiteMetric.parse(line)
                if not raw == BAD_DATA:
                    yield Neo4j5Metric.from_graphite(raw, host, first_seen)
            except Exception as e:
                # XXX drop garbage lines for now
                logging.warning(f"dropping line: {e}")
    except Exception as e:
        logging.warning(f"unhandled parser error: {e}")


async def convert_task(q_in: Queue[WorkItem], q_out: Queue[Neo4j5Metric]) -> None:
    """
    Take raw inbound data and parse into metrics, outputting to q_out.
    """
    while True:
        work = await q_in.get()

        metrics = parse(work.buffer, work.client, CONNECTIONS[work.client])
        for metric in metrics:
            logging.debug(f"adding metric {metric}")
            await q_out.put(metric)

        q_in.task_done()


async def publish_task(q: Queue[Neo4j5Metric],
                       shipper: Callable[[List[Neo4j5Metric]],
                                         Coroutine[Any, Any, None]],
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


def create_consumer(q: Queue[WorkItem]) \
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
        (client, port) = writer.get_extra_info("peername")
        logging.debug(f"got connection from {client}:{port}")
        if client not in CONNECTIONS:
            CONNECTIONS.update({ client: time() })
            logging.info(f"discovered new client {client}")

        # Chug along until EOF, implying disconnect.
        while not reader.at_eof():
            # chunk at lines for now...kinda wasteful here
            line = await reader.readline()
            if line:
                await q.put(WorkItem(buffer=line, client=client))
        logging.debug(f"goodbye {client}:{port}")

    return consumer


async def main(host: str = "127.0.0.1", port: int = 2003) -> None:
    """
    Make rocket go now.
    """
    # Two buckets:
    convert_q: Queue[WorkItem] = Queue()
    publish_q: Queue[Neo4j5Metric] = Queue()

    # The plumbing.
    consumer = create_consumer(convert_q)
    converter = asyncio.create_task(convert_task(convert_q, publish_q),
                                    name="converter")
    publisher = asyncio.create_task(publish_task(publish_q, gcp.write),
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
