# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from enum import Enum
from typing import List, Optional, Dict, Tuple
from pydantic import BaseModel, Field

CONFIG_NAME_REGEX = r"^[a-zA-Z0-9_-]{2,64}$"
REGION_REGEX = r"^[a-zA-Z0-9_-]{2,64}$"

class ReplicaType(str, Enum):
    LEADER = "leader"
    RW_REPLICA = "r/w replica"
    WITNESS = "witness"
    OPTIONAL_READ_ONLY = "optional read only"
    READ_ONLY = "read only"

class InstanceType(str, Enum):
    REGIONAL = "regional"
    DUAL_REGION = "dual-region"
    MULTI_REGION = "multi-region"

class ReplicaInfo(BaseModel):
    region: str = Field(..., pattern=REGION_REGEX)
    location_name: str
    lat: float
    lon: float
    replica_type: ReplicaType
    reads_per_sec: int
    writes_per_sec: int
    is_leader: bool
    is_witness: bool
    is_read_only: bool

class SpannerConfigSummary(BaseModel):
    id: str
    configname: str = Field(..., pattern=CONFIG_NAME_REGEX)
    display_name: str
    instancetype: InstanceType
    continentregion: str
    availability_sla: str
    total_replicas: int
    leader_region: str
    nodes: int = 1
    total_reads_per_sec: int
    total_writes_per_sec: int
    replicas: List[ReplicaInfo] = Field(default_factory=list)

class SpannerConfigDetail(SpannerConfigSummary):
    pass

class ConfigTreeNode(BaseModel):
    key: str
    label: str
    count: Optional[int] = None
    type: Optional[str] = None  # continent, instancetype, config
    children: Optional[List["ConfigTreeNode"]] = None
    configname: Optional[str] = None
    instancetype: Optional[InstanceType] = None
    replica_count: Optional[int] = None

class TopologyNode(BaseModel):
    id: str
    config_name: str = Field(..., pattern=CONFIG_NAME_REGEX)
    region: str = Field(..., pattern=REGION_REGEX)
    location_name: str
    lat: float
    lon: float
    replica_type: ReplicaType
    reads_per_sec: int
    writes_per_sec: int
    sla: str
    nodes: int

class TopologyLink(BaseModel):
    source_id: str
    target_id: str
    source_coord: Tuple[float, float]
    target_coord: Tuple[float, float]
    link_type: str  # leader_to_rw, leader_to_witness, rw_to_witness, leader_to_read_only
    config_name: str
    source_region: Optional[str] = None
    target_region: Optional[str] = None
    distance_km: Optional[float] = None
    distance_miles: Optional[float] = None
    distance_label: Optional[str] = None
    latency_rtt_min_ms: Optional[float] = None
    latency_rtt_typical_ms: Optional[float] = None
    latency_rtt_max_ms: Optional[float] = None
    latency_label: Optional[str] = None
    latency_badge_text: Optional[str] = None
    is_measured: Optional[bool] = None

class DeploymentVisualization(BaseModel):
    nodes: List[TopologyNode]
    links: List[TopologyLink]
    client_links: List[TopologyLink] = Field(default_factory=list)
    bounds: Optional[Tuple[Tuple[float, float], Tuple[float, float]]] = None
    total_reads_per_sec: int
    total_writes_per_sec: int
    configs: List[SpannerConfigDetail]

class VisualizeRequest(BaseModel):
    config_names: List[str] = Field(..., min_length=1, max_length=50)
    nodes_map: Optional[Dict[str, int]] = Field(default_factory=dict)
    leaders_map: Optional[Dict[str, str]] = Field(default_factory=dict)
    client_regions: Optional[List[str]] = Field(default_factory=list)

class CliExportRequest(BaseModel):
    configname: str = Field(..., pattern=CONFIG_NAME_REGEX)
    nodes: int = Field(default=1, ge=1, le=1000)
    leader_region: Optional[str] = None
    optional_replicas: Optional[List[str]] = Field(default_factory=list)

class CliExportResponse(BaseModel):
    configname: str
    gcloud_describe: str
    gcloud_create: str
    terraform_hcl: str
