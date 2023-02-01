from enum import Enum
from time import time
import logging
import requests
import traceback

from google.api import label_pb2
from google.api import metric_pb2
from google.cloud import monitoring_v3

from .metric import *

from typing import cast, Any, Awaitable, Dict, List, NamedTuple, Set, Union


_METAROOT = "http://metadata.google.internal/computeMetadata/v1"
_PROJECT_ID = None
_INSTANCE_ID = None
_ZONE_ID = None
_CLIENT = None

_METRIC_TYPE_ROOT = "custom.googleapis.com/neo4j"


def getProjectId() -> str:
    global _PROJECT_ID
    if _PROJECT_ID is None:
        try:
            _PROJECT_ID = requests.get(
                f"{_METAROOT}/project/project-id",
                headers={"Metadata-Flavor": "Google"}
            ).text
            logging.info(f"using PROJECT_ID={_PROJECT_ID}")
        except Exception as e:
            logging.warning(f"failed to fetch project id: {e}")
    return cast(str, _PROJECT_ID)


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


async def create_metric_descriptor(metric: Neo4j5Metric) -> bool:
    """
    Given a Neo4j5Metric, tell GCP some details on what it represents.

    This is optional, but lets us define some metadata for labels.
    """
    client = getClient()
    project_name = f"projects/{getProjectId()}"

    desc = metric_pb2.MetricDescriptor()
    desc.type = f"{_METRIC_TYPE_ROOT}/{metric.key}"

    if metric.kind == MetricKind.COUNTER:
        desc.metric_kind = metric_pb2.MetricDescriptor.MetricKind.CUMULATIVE
    elif metric.kind == MetricKind.GAUGE:
        desc.metric_kind = metric_pb2.MetricDescriptor.MetricKind.GAUGE
    else:
        desc.metric_kind = (
            metric_pb2.MetricDescriptor.MetricKind.METRIC_KIND_UNSPECIFIED
        )

    if metric.value_type == ValueType.INT:
        desc.value_type = metric_pb2.MetricDescriptor.ValueType.INT64
    elif metric.value_type == ValueType.FLOAT:
        desc.value_type = metric_pb2.MetricDescriptor.ValueType.DOUBLE
    else:
        desc.value_type = (
            metric_pb2.MetricDescriptor.ValueType.VALUE_TYPE_UNSPECIFIED
        )

    for label in metric.labels:
        label_desc = label_pb2.LabelDescriptor()
        label_desc.key = label.descriptor.key
        # set everything to string for now
        label_desc.value_type = label_pb2.LabelDescriptor.ValueType.STRING
        desc.labels.append(label_desc)

    try:
        await client.create_metric_descriptor(
            name=project_name, metric_descriptor=desc
        )
        return True
    except Exception as e:
        logging.error(f"failed to create metric descriptor: {e}")
        traceback.print_exc()
    return False


def create_time_series(metric: Neo4j5Metric) -> monitoring_v3.TimeSeries:
    """
    Create a single GCP Time Series object.
    """
    series = monitoring_v3.TimeSeries()

    # Define the TimeInterval...
    # for gauges, the start time is optional, but otherwise must be the end time
    end_seconds = int(metric.seen_at)
    end_time = {
        "seconds": end_seconds,
        "nanos": int((metric.seen_at - end_seconds) * 10**9),
    }
    start_time = end_time
    if metric.kind == MetricKind.COUNTER:
        start_seconds = int(metric.origin.first_seen)
        start_time = {
            "seconds": start_seconds,
            "nanos": int((metric.origin.first_seen - start_seconds) * 10**9),
        }
    interval = monitoring_v3.TimeInterval({
        "start_time": start_time,
        "end_time": end_time
    })

    # Describe the series metadata...
    series.metric.type = f"{_METRIC_TYPE_ROOT}/{metric.key}"
    series.resource.type = "gce_instance"
    # series.resource.labels["instance_id"] = getInstanceId()
    # series.resource.labels["zone"] = getZoneId()

    for label in metric.labels:
        series.metric.labels[label.descriptor.key] = label.value

    # Assign our value as a point...
    point_value: Dict[str, Any] = {}
    if metric.value_type == ValueType.INT:
        point_value = {"int64_value": int(metric.value)}
    else:
        point_value = {"double_value": float(metric.value)}

    point = monitoring_v3.Point({"interval": interval,
                                 "value": point_value})
    series.points = [point]
    return series


async def write_time_series(series: List[monitoring_v3.TimeSeries]) -> bool:
    """
    Crude first cut at writing a TimeSeries metric. Still needs work:
      - handle multiple values at a time?
      - deal with proper start time setting?
    """
    client = getClient()
    project_name = f"projects/{getProjectId()}"

    try:
        await client.create_time_series(name=project_name,
                                        time_series=series)
        return True
    except Exception as e:
        logging.error(f"failed to create time series: {e}")
        traceback.print_exc()
    return False



_METRICS: Set[str] = set()

async def write(metrics: List[Neo4j5Metric]) -> None:
    """
    Send the metrics to GCP!
    """
    series = []

    for metric in metrics:

        # Check if we haven't defined this metric type yet
        if not metric.key in _METRICS:
            _METRICS.add(metric.key)
            logging.info(f"defining new metric: {metric})")
            if await create_metric_descriptor(metric):
                logging.info(f"created descriptor for metric {metric}")
            else:
                logging.warning(f"failed to create descriptor for {metric}")

        # Convert into a time series
        time_series = create_time_series(metric)
        series.append(time_series)

    if await write_time_series(series):
        logging.info(f"wrote {len(series)} time series to GCP")
    else:
        logging.warning(f"failed to write time series to GCP!")
