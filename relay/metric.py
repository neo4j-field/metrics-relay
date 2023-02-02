"""
Platform-independent Metric Data Model
"""
from enum import Enum
from typing import Dict, List, NamedTuple, Union


def safe_convert(s: str) -> Union[int, float]:
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


class GraphiteMetric(NamedTuple):
    """
    Represents the raw text-based value off the wire.
    """
    key: str
    value: Union[int, float]
    seen_at: Union[int, float]

    @staticmethod
    def parse(s: str) -> "GraphiteMetric":
        try:
            (key, val, ts) = s.strip().split(" ")
            return GraphiteMetric(key=key,
                                  value=safe_convert(val),
                                  seen_at=safe_convert(ts))
        except Exception as e:
            # just toss for now
            raise e


class MetricKind(Enum):
    """
    Each metric is represented semantically as either:
      COUNTER: monotonicaly increasing value
      GAUGE: value that changes in time
    """
    COUNTER = "COUNTER"
    GAUGE = "GAUGE"


class ValueType(Enum):
    """
    Each metric can report a value in different types. Neo4j emits:
      INT: signed 64-bit integer values
      FLOAT: decimal/floating point values
    """
    INT = "INT"
    FLOAT = "FLOAT"


class MetricSystem(Enum):
    """
    Which system does it pertain to?
    """
    DBMS = "DBMS"
    DATABASE = "DATABASE"
    POOL = "POOL"


class LabelType(Enum):
    """
    Metrics labels are typed, at least in GCP.

    XXX: remove this? Why wouldn't we just use STRING?
    """
    BOOL = "BOOL"
    INT = "INT"
    STRING = "STRING"


class LabelDescriptor(NamedTuple):
    """
    Metrics can be described by Labels to provide details on what they
    represent. A LabelDescriptor defines valid labels and their details.
    """
    key: str
    value_type: LabelType = LabelType.STRING
    description: str = ""


class Label(NamedTuple):
    """
    """
    descriptor: LabelDescriptor
    value: Union[str, int, bool]

    def as_dict(self) -> Dict[str, Union[str, int, bool]]:
        return { self.descriptor.key: self.value }


# Global value representing a bogus metric instance.
BAD_DATA = GraphiteMetric("", -1, -1)


CLIENT_NAME_LABEL = LabelDescriptor(key="neo4j_instance_name",
                                    value_type=LabelType.STRING,
                                    description="Hostname of Neo4j system")
DB_NAME_LABEL = LabelDescriptor(key="neo4j_db_name",
                                value_type=LabelType.STRING,
                                description="Name of the Neo4j database.")
SYSTEM_LABEL = LabelDescriptor(key="neo4j_system",
                               value_type=LabelType.STRING,
                               description="Subsystem reporting the metric.")
HOST_LABEL = LabelDescriptor(key="neo4j_label",
                             value_type=LabelType.STRING,
                             description="Label applied to Neo4j host.")
# TODO: add some metric classes, e.g. page cache, heap, etc.


class Origin(NamedTuple):
    host: str
    label: str
    first_seen: float


# Known Counter values. TODO: turn into dict with descriptions.
COUNTERS = set([
    # Bolt
    "connections_opened", "connections_closed",
    "messages_received", "messages_started", "messages_done", "messages_failed",
    "accumulated_queue_time", "accumulated_processing_time",

    # Checkpointing
    "events", "total_time",

    # Cypher
    "replan_events", "replan_wait_time",

    # DB operation counts
    "create", "start", "stop", "drop", "failed", "recovered",

    # Page cache
    "eviction_exceptions", "flushes", "merges", "unpins", "pins", "evictions",
    "cooperative", "page_faults", "page_fault_failures",
    "page_cancelled_faults", "hits", "bytes_read", "bytes_written",
    "pages_copies",

    # Query execution
    "success", "failure",

    # DB Transaction log metrics
    "rotation_events", "rotation_total_time", "appended_bytes", "flushes",

    # DB transaction metrics
    "started", "peak_concurrent", "committed", "committed_read",
    "committed_write", "rollbacks", "rollbacks_read", "rollbacks_write",
    "terminated", "terminated_read", "terminated_write", "committed_tx_id",
    "last_closed_tx_id",

    # Clustering
    "pull_resusts_received", "tx_retries", "hits", "misses",
    "message_processing_timer", "replication_attempt", "replication_fail",
    "replication_maybe", "replication_success",

    # Secondary db metrics
    "pull_updates", "pull_updates_highest_tx_id_requested",
    "pull_updates_highest_tx_id_received",

    # JVM
    "pause_time",

    # the obvious
    "count",
])


class Neo4j5Metric:
    """
    Represents a metric emitted by Neo4j v5.
    """
    key: str
    value: Union[int, float]
    origin: Origin
    seen_at: Union[int, float]

    kind: MetricKind = MetricKind.GAUGE
    value_type: ValueType = ValueType.INT
    system: MetricSystem = MetricSystem.DBMS
    labels: List[Label] = []


    @staticmethod
    def from_graphite(raw: GraphiteMetric, host: str,
                      first_seen: float = 0.0) -> "Neo4j5Metric":
        parts = raw.key.split(".")
        metric = Neo4j5Metric()
        metric.labels = []

        # copy some simple values
        metric.value = raw.value
        metric.origin = Origin(host=host,
                               label=parts[0], # it's our prefix
                               first_seen=min(first_seen, raw.seen_at))
        metric.seen_at = raw.seen_at

        # identify the neo4j instance
        metric.labels.append(Label(CLIENT_NAME_LABEL, metric.origin.host))
        metric.labels.append(Label(HOST_LABEL, metric.origin.label))

        # identify the subsystem
        if parts[1] == "database":
            metric.system = MetricSystem.DATABASE
            metric.labels.append(Label(DB_NAME_LABEL, parts[2]))
            metric.labels.append(Label(SYSTEM_LABEL, "database"))
        else:
            # just assign DBMS for now...should break out into subsystems
            metric.system = MetricSystem.DBMS
            metric.labels.append(Label(SYSTEM_LABEL, "dbms"))

        # identify the metric kind
        if parts[-1] in COUNTERS:
            metric.kind = MetricKind.COUNTER
        else:
            metric.kind = MetricKind.GAUGE

        # identify the metric type based on the value class
        if type(metric.value) is int:
            metric.value_type = ValueType.INT
        else:
            metric.value_type = ValueType.FLOAT

        # lastly, construct the key value...dropping some prefix junk
        if metric.system == MetricSystem.DATABASE:
            # drop "<label>.database.<db_name>..."
            metric.key = "/".join(parts[3:])
        else:
            # drop "<label>.dbms...."
            metric.key = "/".join(parts[2:])

        return metric


    def __str__(self) -> str:
        return (
            f"Neo4j5Metric(key={self.key}, value={self.value}, " +
            f"origin={self.origin}, seen_at={self.seen_at}, " +
            f"kind={self.kind}, system={self.system}, labels={self.labels}"
        )
