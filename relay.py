#!/usr/bin/env python3
"""
    ``You miss 100% of the shots you don't take.''
                       -- Wayne Gretzky
                          -- Michael Scott
"""
import asyncio
from typing import Coroutine


def parse(data: bytes):
    """
    Take a given series of bytes and turn into a series of Metrics.
    """
    try:
        for line in data.decode("utf8").splitlines():
            try:
                (metric, val, ts) = line.strip().split(" ")
                print(f"metric: {metric}, value: {val}, time: {int(ts)}")
            except Exception as e:
                # XXX drop garbage for now
                print(e)
                pass
    except Exception as e:
        print(e)
        pass # XXX


async def echo_task(q: asyncio.Queue):
    while True:
        item = await q.get()
        parse(item)
        q.task_done()


def create_consumer(q: asyncio.Queue) -> Coroutine:
    """
    Capture a given asyncio Queue instance and return a new coroutine that
    consumes from said queue.
    """
    async def consumer(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """
        Primary connection handler.
        """
        peer = writer.get_extra_info("peername")
        print(f"got connection from {peer}")

        # Is there a simpler way to do this?
        while True:
            line = await reader.readline()
            if not line:
                return
            await q.put(line)
    return consumer


async def main(host: str = "localhost", port: int = 2003):
    """
    Make rocket go now.
    """
    queue = asyncio.Queue()

    consumer = create_consumer(queue)
    server = await asyncio.start_server(consumer, host=host, port=port)
    addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
    print(f'listening on: {addrs}')

    async with server:
        # spin up our processor
        asyncio.create_task(echo_task(queue), name="consumer")
        # now spin up our network consumer
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # ctrl-c caught
        pass
