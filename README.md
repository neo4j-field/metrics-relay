# metrics-relay
[![mypy](https://github.com/neo4j-field/metrics-relay/actions/workflows/mypy.yml/badge.svg?branch=main)](https://github.com/neo4j-field/metrics-relay/actions/workflows/mypy.yml)

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
usage: metrics-relay [-h] [-d] [-s] [--host HOST] [--port PORT]

Sends Neo4j metrics to the Cloud

options:
  -h, --help   show this help message and exit
  -d, --debug  turn on debug logging
  -s, --simple  simplify logging output for journald
  --host HOST  hostname or ip to listen on
  --port PORT  tcp port to listen on
```

## Running as a Service
First, either clone or download a release to a location of your
choosing. The default is currently `/usr/local/src/metrics-relay`.

Make sure you've created a virtualenv and installed the dependencies
as mentioned in the above [Install][#install] section.

Install the provided unit file and enable it at boot:

```
# cp metrics-relay.service /etc/systemd/system/
# systemctl enable metrics-relay.service
```

To start:

```
# systemctl start metrics-relay.service
```

The unit file is preconfigured to turn down the logging complexity so
you can view the output using `journalctl`:

```
# journalctl -u metrics-relay.service
```

## Metrics & You
The app should add labels to your metrics, including:

- `neo4j_label`: the label you set in `server.metrics.graphite.prefix`
- `neo4j_system`: the reporting subsystem (i.e. dbms, db)
- `neo4j_db_name`: name of the database, if applicable
- `neo4j_instance_name`: ip/hostname of the client reporting metrics

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
