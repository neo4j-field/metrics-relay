from enum import Enum
from time import time
import logging
import requests

from google.api.label_pb2 import LabelDescriptor
from google.api.metric_pb2 import Metric, MetricDescriptor
from google.cloud import monitoring_v3

from typing import cast, Any, Awaitable, Dict, List, NamedTuple


_METAROOT = "http://metadata.google.internal/computeMetadata/v1"
_PROJECT_ID = None
_INSTANCE_ID = None
_ZONE_ID = None
_CLIENT = None

_METRIC_TYPE_ROOT = "custom.googleapis.com/neo4j"


## TODO: pull these out as a global abstraction
class MetricKind(Enum):
    COUNTER = "COUNTER"
    DELTA = "DELTA"
    GAUGE = "GAUGE"
    UNDEFINED = "UNDEFINED"

class MetricType(Enum):
    BOOL = "BOOL"
    DISTRIBUTION = "DISTRIBUTION"
    INT = "INT"
    FLOAT = "FLOAT"
    STRING = "STRING" # XXX UNSUPPORTED?
    UNDEFINED = "UNDEFINED"

class LabelValueType(Enum):
    BOOL = "BOOL"
    INT = "INT"
    STRING = "STRING"

class MetricLabel(NamedTuple):
    key: str
    value_type: LabelValueType = LabelValueType.STRING
    description: str = ""


def getProjectId() -> str:
    global _PROJECT_ID
    if _PROJECT_ID is None:
        try:
            _PROJECT_ID = requests.get(
                f"{_METAROOT}/project/project-id",
                headers={"Metadata-Flavor": "Google"}
            ).text
        except Exception as e:
            logging.warning(f"failed to fetch project id: {e}")
    return cast(str, _PROJECT_ID)


def getProjectName() -> str:
    return f"projects/${getProjectId()}"


def getInstanceId() -> str:
    global _INSTANCE_ID
    if _INSTANCE_ID is None:
        try:
            _INSTANCE_ID = requests.get(
                f"{_METAROOT}/instance/id",
                headers={"Metadata-Flavor": "Google"}
            ).text
        except Exception as e:
            logging.warning(f"failed to fetch instance id: {e}")
    return cast(str, _INSTANCE_ID)


def getZoneId() -> str:
    global _ZONE_ID
    if _ZONE_ID is None:
        try:
            _ZONE_ID = (
                requests.get(f"{_METAROOT}/instance/zone",
                             headers={"Metadata-Flavor": "Google"})
                .text
                .split("/")[-1]
            )
        except Exception as e:
            logging.warning(f"failed to fetch zone id: {e}")
    return cast(str, _ZONE_ID)


def getClient() -> monitoring_v3.MetricServiceAsyncClient:
    global _CLIENT
    if _CLIENT is None:
        try:
            _CLIENT = monitoring_v3.MetricServiceAsyncClient()
        except Exception as e:
            logging.warning(f"failed to create MetricServiceAsyncClient: {e}")
    return cast(monitoring_v3.MetricServiceAsyncClient, _CLIENT)


async def create_metric_descriptor(name: str,
                                   metric_kind: MetricKind,
                                   value_type: MetricType,
                                   labels: List[MetricLabel] = []) \
                                   -> Awaitable[Any]:
    client = getClient()
    desc = MetricDescriptor()
    desc.type = f"{_METRIC_TYPE_ROOT}/{name}"

    if metric_kind == MetricKind.COUNTER:
        desc.metric_kind = MetricDescriptor.MetricKind.CUMULATIVE
    elif metric_kind == MetricKind.DELTA:
        desc.metric_kind = MetricDescriptor.MetricKind.DELTA
    elif metric_kind == MetricKind.GAUGE:
        desc.metric_kind = MetricDescriptor.MetricKind.GAUGE
    else:
        desc.metric_kind = MetricDescriptor.MetricKind.METRIC_KIND_UNSPECIFIED

    if value_type == MetricType.BOOL:
        desc.value_type = MetricDescriptor.ValueType.BOOL
    elif value_type == MetricType.INT:
        desc.value_type = MetricDescriptor.ValueType.INT
    elif value_type == MetricType.FLOAT:
        desc.value_type = MetricDescriptor.ValueType.DOUBLE
    elif value_type == MetricType.DISTRIBUTION:
        desc.value_type = MetricDescriptor.ValueType.DISTRIBUTION
    elif value_type == MetricType.STRING:
        desc.value_type = MetricDescriptor.ValueType.STRING
    else:
        desc.value_type = MetricDescriptor.ValueType.VALUE_TYPE_UNSPECIFIED

    for label in labels:
        l = LabelDescriptor()
        l.key = label.key
        if label.value_type == LabelValueType.BOOL:
            l.value_type = LabelDescriptor.ValueType.BOOL
        elif label.value_type == LabelValueType.INT:
            l.value_type = LabelDescriptor.ValueType.INT
        else:
            l.value_type = LabelDescriptor.ValueType.STRING
        desc.labels.append(l)

    return client.create_metric_descriptor(
        name=getProjectName(), metric_descriptor=desc
    )


async def write_time_series(name: str, value: Any, value_type: MetricType,
                            labels: Dict[str, str] = {}) -> Awaitable[Any]:
    """
    Crude first cut at writing a TimeSeries metric. Still needs work:
      - handle multiple values at a time?
      - deal with proper start time setting?
    """
    client = getClient()
    now = time()
    interval = monitoring_v3.TimeInterval({
        "end_time": {
            "seconds": int(now),
            "nanos": (int((now - int(now)) * 10**9))
        }
    })

    series = monitoring_v3.TimeSeries()
    series.metric.type = f"{_METRIC_TYPE_ROOT}/{name}"
    series.resource.type = "gce_instance"
    series.resource.labels["instance_id"] = getInstanceId()
    series.resource.labels["zone"] = getZoneId()

    for key, value in labels.items():
        series.metric.labels[key] = value

    v: Dict[str, Any] = {}
    if value_type == MetricType.INT:
        v = {"int_value": int(value)}
    elif value_type == MetricType.FLOAT:
        v = {"double_value": float(value)}
    elif value_type == MetricType.STRING:
        v = {"string_value": str(value)}
    elif value_type == MetricType.BOOL:
        v = {"bool_value": bool(value)}
    else:
        # FALLBACK
        v = {"string_value": str(value)}

    point = monitoring_v3.Point({"interval": interval,
                                 "value": v})
    series.points = [point]
    return client.create_time_series(name=getProjectName(),
                                     time_series=[series])