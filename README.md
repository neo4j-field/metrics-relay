# metrics-relay

>     ``You miss 100% of the shots you don't take.''
>                       -- Wayne Gretzky
>                          -- Michael Scott

Send your Neo4j metrics to the cloud!

## Install
I recommend a virtual environment.

```
$ python3 -m venv venv
$ . venv/bin/activate
(venv) $ pip install -U pip
(venv) $ pip install -r requirements.txt
```

## Configuring Neo4j
You need a **Neo4j 5 Enterprise** instance. (It may or may not work
with v4.x.). Enable metrics and configure the `graphite` metrics
support like so in `neo4j.conf` or the like:

```properties
server.metrics.graphite.enabled=true
server.metrics.graphite.server=${hostname:port}
server.metrics.graphite.interval=30s
server.metrics.graphite.prefix=${your_neo4j_instance_name}
```

Replacing the `${hostname:port}` with the network info to reach your
`metrics-relay` app and make sure to set `${your_neo4j_instance_name}`
to something identifable to distinguish the source of data.

> NOTE: you may also want to set `server.metrics.filter=*` to unmask
> all metrics.

## Running
Assumes you're running on a GCP VM or GKE container. (Maybe we add
other clouds later.)

Simple, assuming you've activated your virtual environment:

```
$ python main.py
```

You have a few optional config options:

```
usage: metrics-relay [-h] [-d] [--host HOST] [--port PORT]

Sends Neo4j metrics to the Cloud

options:
  -h, --help   show this help message and exit
  -d, --debug  turn on debug logging
  --host HOST  hostname or ip to listen on
  --port PORT  tcp port to listen on
```

## Hacking
Want to do me a favor and add another cloud platform? Take a look at
`relay/gcp.py`. The API is still congealing, but in short you just
need to implement an async function that looks like:

```python
async def write_to_my_cloud(metrics: List[Neo4j5Metric]) -> None:
    pass
```

## Async What Now?
Yeah, this uses Python's `asyncio`. Works shockingly well for this
type of application once you get your head around the annoyances of
async/await in Python land.

## License & Copywrite
Unless otherwise noted, this project is (c) Neo4j, Inc and provided
under the terms of the Apache License 2.0. No warranty or support.
