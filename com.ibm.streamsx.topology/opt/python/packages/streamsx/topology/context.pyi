# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017, 2019
from typing import Any, Optional

from streamsx.topology.topology import Topology

def submit(ctxtype: Any, graph: Topology, config: Any=None, username: str=None, password: str=None) -> SubmissionResult: ...

class JobConfig(object):
    def __init__(self, job_name: str=None, job_group: str=None, preload: Any=bool, data_directory: str=None, tracing: Any=None) -> None:
        self.job_name : Optional[str]
        self.job_group : Optional[str]
        self.preload : Optional[bool]
        self.data_directory : Optional[bool]
        self.tracing : Any
        self.target_pe_count : Optional[int]
        self.raw_overlay : Optional[dict]
        self.comment = Optional[str]

    @staticmethod
    def from_overlays(overlays: dict) -> JobConfig:

    def add(self, config: dict) -> dict: ...
    def as_overlays(self) -> dict: ...

class SubmissionResult(object):
    def __init__(self, results: Any) -> None: ...
    def job(self) -> Optional[Any]: ...

class ContextTypes(object):
    STREAMING_ANALYTICS_SERVICE : str
    DISTRIBUTED : str
    STANDALONE : str
    BUNDLE : str
    TOOLKIT : str
    BUILD_ARCHIVE : str
    EDGE : str

class ConfigParams(object):
    VCAP_SERVICES : str
    SERVICE_NAME : str
    FORCE_REMOTE_BUILD : str
    JOB_CONFIG : str
    STREAMS_CONNECTION : str
    SERVICE_DEFINITION : str

