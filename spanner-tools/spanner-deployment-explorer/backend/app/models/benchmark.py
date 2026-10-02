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
from typing import Dict, List, Optional
from pydantic import BaseModel, Field


class CampaignState(str, Enum):
    DRAFT = "draft"
    IDLE = "idle"
    PREFLIGHT_CHECK = "preflight_check"
    PROVISIONING_SPANNER = "provisioning_spanner"
    PROVISIONING_VMS = "provisioning_vms"
    RUNNING = "running"
    COMPLETED = "completed"
    STOPPED = "stopped"
    FAILED = "failed"
    DELETING = "deleting"


class LatencyMetrics(BaseModel):
    ops_count: int = Field(default=0, description="Total number of operations executed")
    hit_rate: float = Field(default=1.0, description="Fraction of read attempts that returned a row (1.0 = 100% hits)")
    p50: float = Field(default=0.0, description="50th percentile latency (ms)")
    p90: float = Field(default=0.0, description="90th percentile latency (ms)")
    p95: float = Field(default=0.0, description="95th percentile latency (ms)")
    p99: float = Field(default=0.0, description="99th percentile latency (ms)")
    min: float = Field(default=0.0, description="Minimum latency (ms)")
    max: float = Field(default=0.0, description="Maximum latency (ms)")
    avg: float = Field(default=0.0, description="Average latency (ms)")


class RegionBenchmarkResult(BaseModel):
    region: str = Field(..., description="GCE client region identifier, e.g. us-central1")
    region_name: str = Field(..., description="Human-friendly region name, e.g. Iowa")
    distance_to_leader_km: float = Field(default=0.0, description="Distance to Spanner leader region in km")
    has_local_replica: bool = Field(default=False, description="True if a read-serving replica (leader/read-write/read-only, never witness) is present in this region")
    replica_type: Optional[str] = Field(default=None, description="Spanner replica role: 'Leader Region', 'R/W Replica Region', 'Witness Region', 'Read Only Region', or None")
    direct_access: bool = Field(default=True, description="True if Direct Access is enabled (direct gRPC to Spanner backend)")
    direct_path_confirmed: Optional[bool] = Field(default=None, description="True if DirectPath ALTS was confirmed active; False if using GFE proxy; None if unverified")
    status: str = Field(default="pending", description="Region run status: pending, provisioning, running, completed, failed")
    progress_pct: int = Field(default=0, ge=0, le=100, description="Progress for this region's benchmark run")
    writes: LatencyMetrics = Field(default_factory=LatencyMetrics, description="Single-row write latency metrics")
    strong_reads: LatencyMetrics = Field(default_factory=LatencyMetrics, description="Point lookup strong read latency metrics")
    stale_reads: LatencyMetrics = Field(default_factory=LatencyMetrics, description="Point lookup stale read (15s) latency metrics")
    error_message: Optional[str] = Field(None, description="Error message if region run failed")


class StepStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class BenchmarkStep(BaseModel):
    id: str = Field(..., description="Unique step identifier")
    name: str = Field(..., description="Human-friendly step title")
    description: str = Field(..., description="Step description")
    status: StepStatus = Field(default=StepStatus.PENDING, description="Current status of step")
    started_at: Optional[str] = Field(None, description="Start timestamp")
    completed_at: Optional[str] = Field(None, description="Completion timestamp")
    duration_seconds: Optional[float] = Field(None, description="Execution duration in seconds")
    error_message: Optional[str] = Field(None, description="Error detail if step failed")


class BenchmarkLogEntry(BaseModel):
    timestamp: str = Field(..., description="Log entry timestamp")
    level: str = Field(default="INFO", description="Log level: INFO, WARN, ERROR, SUCCESS")
    stage: str = Field(default="system", description="Execution phase/stage")
    message: str = Field(..., description="Log message text")


class BenchmarkCampaignConfig(BaseModel):
    name: Optional[str] = Field(default="", description="User-friendly benchmark test name")
    description: Optional[str] = Field(default="", description="Optional benchmark notes or description")
    project_id: str = Field(..., min_length=4, max_length=30, description="Target GCP project ID")
    spanner_config: str = Field(..., description="Target Spanner instance configuration")
    leader_region: Optional[str] = Field(default=None, description="Identified or custom leader region for Spanner instance")
    client_regions: List[str] = Field(..., min_length=1, description="List of GCE client regions to benchmark from")
    operations: int = Field(default=1000, ge=10, le=10000, description="Number of operations per benchmark test")
    staleness_seconds: int = Field(default=15, ge=1, le=3600, description="Staleness bound for stale reads (seconds)")
    execution_mode: str = Field(default="dry_run", description="Execution mode: 'live' or 'dry_run'")
    auto_teardown: bool = Field(default=True, description="Auto-delete Spanner instance and resources upon test completion")
    staging_bucket: Optional[str] = Field(default=None, description="Optional user-specified GCS staging bucket (e.g. 'my-bucket' or 'gs://my-bucket')")
    optional_replicas: List[str] = Field(default_factory=list, description="Optional read-only replicas to provision")


class BenchmarkCampaignUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    project_id: Optional[str] = None
    spanner_config: Optional[str] = None
    leader_region: Optional[str] = None
    client_regions: Optional[List[str]] = None
    operations: Optional[int] = None
    staleness_seconds: Optional[int] = None
    execution_mode: Optional[str] = None
    auto_teardown: Optional[bool] = None
    staging_bucket: Optional[str] = None
    optional_replicas: Optional[List[str]] = None


class BenchmarkCampaignStatus(BaseModel):
    campaign_id: str = Field(..., description="Unique campaign ID")
    name: Optional[str] = Field(default="", description="User-friendly benchmark test name")
    description: Optional[str] = Field(default="", description="Optional benchmark notes or description")
    status: CampaignState = Field(..., description="Overall campaign lifecycle state")
    progress_pct: int = Field(default=0, ge=0, le=100, description="Overall campaign progress percentage")
    project_id: str = Field(..., description="Target GCP Project ID")
    spanner_instance_id: Optional[str] = Field(None, description="Provisioned Spanner instance ID")
    spanner_database_id: Optional[str] = Field(None, description="Provisioned Spanner database ID")
    spanner_config: str = Field(..., description="Spanner instance configuration name")
    leader_region: Optional[str] = Field(None, description="Identified leader region for Spanner instance")
    client_regions: List[str] = Field(default_factory=list, description="Client regions configured for this benchmark")
    optional_replicas: List[str] = Field(default_factory=list, description="Optional read-only replicas provisioned for this benchmark")
    operations: int = Field(default=1000, description="Benchmark operations count")
    staleness_seconds: int = Field(default=15, description="Staleness seconds bound")
    message: str = Field(default="", description="Current status message")
    execution_mode: str = Field(..., description="'live' or 'dry_run'")
    auto_teardown: bool = Field(default=True, description="Whether resources are automatically cleaned up on completion")
    staging_bucket: Optional[str] = Field(default=None, description="GCS staging bucket used for runner artifacts")
    gce_instances: Dict[str, str] = Field(default_factory=dict, description="Provisioned GCE client VMs keyed by region ('region' -> 'instance_name:zone')")
    steps: List[BenchmarkStep] = Field(default_factory=list, description="Lifecycle steps executed for this campaign")
    logs: List[BenchmarkLogEntry] = Field(default_factory=list, description="Timestamped execution logs")
    region_results: Dict[str, RegionBenchmarkResult] = Field(default_factory=dict, description="Results keyed by client region")
    created_at: str = Field(..., description="Campaign creation ISO timestamp")
    completed_at: Optional[str] = Field(None, description="Campaign completion ISO timestamp")



class ClientRegionInfo(BaseModel):
    region: str = Field(..., description="GCE region identifier")
    name: str = Field(..., description="Region location name")
    continent: str = Field(..., description="Continent grouping")
    latitude: float = Field(..., description="Latitude coordinate")
    longitude: float = Field(..., description="Longitude coordinate")
