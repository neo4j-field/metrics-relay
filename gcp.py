import logging

import requests

from google.api import label_pb2 as ga_label
from google.api import metric_pb2 as ga_metric
from google.cloud import monitoring_v3 as ga_monitoring


_METAROOT = "http://metadata.google.internal/computeMetadata/v1"
_PROJECT_ID = None
_INSTANCE_ID = None
_ZONE_ID = None
_CLIENT = None


def getProjectId() -> str:
    global _PROJECT_ID
    if _PROJECT_ID is None:
        try:
            _PROJECT_ID = requests.get(
                f"{_METAROOT}/project/project-id",
                headers={"Metadata-Flavor": "Google"}
            ).text
        except Exception as e:
            logger.warning("failed to fetch project id: {e}")
    return _PROJECT_ID


def getInstanceId() -> str:
    global _INSTANCE_ID
    if _INSTANCE_ID is None:
        try:
            _INSTANCE_ID = requests.get(
                f"{_METAROOT}/instance/id",
                headers={"Metadata-Flavor": "Google"}
            ).text
        except Exception as e:
            logger.warning("failed to fetch instance id: {e}")
    return _INSTANCE_ID


def getZoneId() -> str:
    global _ZONE_ID
    if _ZONE_ID is None:
        try:
            _ZONE_ID = requests.get(
                f"{_METAROOT}/instance/zone",
                headers={"Metadata-Flavor": "Google"}
            ).text
        except Exception as e:
            logger.warning("failed to fetch zone id: {e}")
    return _ZONE_ID
