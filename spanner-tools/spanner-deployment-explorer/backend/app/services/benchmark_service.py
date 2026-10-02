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

import asyncio
import json
import logging
import math
import os
import random
import shutil
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from app.models.benchmark import (
    BenchmarkCampaignConfig,
    BenchmarkCampaignStatus,
    BenchmarkCampaignUpdate,
    BenchmarkLogEntry,
    BenchmarkStep,
    CampaignState,
    ClientRegionInfo,
    LatencyMetrics,
    RegionBenchmarkResult,
    StepStatus,
)
from app.models.preflight import PreflightCheckRequest
from app.models.spanner import ReplicaType
from app.services.preflight_service import PreflightService, find_gcloud_binary
from app.services.spanner_service import SpannerService

logger = logging.getLogger("dbexplorer.benchmark_service")


def _init_default_steps() -> List[BenchmarkStep]:
    return [
        BenchmarkStep(
            id="preflight",
            name="1. Pre-flight Verification",
            description="Verifying ADC credentials, project validity, Spanner & Compute APIs, and IAM permissions",
            status=StepStatus.PENDING,
        ),
        BenchmarkStep(
            id="provision_spanner",
            name="2. Cloud Spanner Provisioning",
            description="Provisioning 1-node instance (1000 PUs) in target configuration",
            status=StepStatus.PENDING,
        ),
        BenchmarkStep(
            id="create_database",
            name="3. Database & Schema Initialization",
            description="Creating database and schema table 'LatencyKV' with primary key indexing",
            status=StepStatus.PENDING,
        ),
        BenchmarkStep(
            id="setup_clients",
            name="4. Client Runner Setup",
            description="Provision load test runner across client locations",
            status=StepStatus.PENDING,
        ),
        BenchmarkStep(
            id="execute_workload",
            name="5. Latency Workload Execution",
            description="Running load test",
            status=StepStatus.PENDING,
        ),
        BenchmarkStep(
            id="aggregate_metrics",
            name="6. Telemetry & Results Analysis",
            description="Calculating latency percentiles (p50, p90, p95, p99, min, max, avg)",
            status=StepStatus.PENDING,
        ),
        BenchmarkStep(
            id="teardown",
            name="7. Resource Teardown",
            description="Tearing down database and Cloud Spanner instance to prevent ongoing billing",
            status=StepStatus.PENDING,
        ),
    ]

# Storage path for persistent benchmark tests
BACKEND_DIR = Path(__file__).resolve().parent.parent.parent


def find_mvn_binary() -> Optional[str]:
    """Finds mvn (Maven) binary in PATH or common installation paths."""
    found = shutil.which("mvn")
    if found:
        return found
    common_paths = [
        Path.home() / "homebrew" / "bin" / "mvn",
        Path.home() / "homebrew" / "Cellar" / "maven" / "3.9.0" / "bin" / "mvn",
        Path("/opt/homebrew/bin/mvn"),
        Path("/usr/local/bin/mvn"),
        Path("/usr/bin/mvn"),
    ]
    for p in common_paths:
        if p.exists() and os.access(p, os.X_OK):
            return str(p)
    return None


def _resolve_data_dir() -> Path:
    return Path(os.environ.get("DBEXPLORER_DATA_DIR") or (BACKEND_DIR / "data"))

# Comprehensive GCE client regions with coordinates and geographical metadata
CLIENT_REGIONS_CATALOG: Dict[str, Tuple[str, str, float, float]] = {
    # Americas
    "us-central1": ("Iowa", "North America", 41.2619, -95.8608),
    "us-east1": ("South Carolina", "North America", 33.1960, -79.9740),
    "us-east4": ("Northern Virginia", "North America", 39.0438, -77.4874),
    "us-east5": ("Columbus", "North America", 39.9612, -82.9988),
    "us-south1": ("Dallas", "North America", 32.7767, -96.7970),
    "us-west1": ("Oregon", "North America", 45.5946, -121.1786),
    "us-west2": ("Los Angeles", "North America", 34.0522, -118.2437),
    "us-west3": ("Salt Lake City", "North America", 40.7608, -111.8910),
    "us-west4": ("Las Vegas", "North America", 36.1699, -115.1398),
    "us-west8": ("Phoenix", "North America", 33.4484, -112.0740),
    "northamerica-northeast1": ("Montréal", "North America", 45.5017, -73.5673),
    "northamerica-northeast2": ("Toronto", "North America", 43.6532, -79.3832),
    "northamerica-south1": ("Querétaro", "North America", 20.5888, -100.3899),
    "southamerica-east1": ("São Paulo", "South America", -23.5505, -46.6333),
    "southamerica-west1": ("Santiago", "South America", -33.4489, -70.6693),
    # Europe
    "europe-west1": ("Belgium", "Europe", 50.4542, 3.8232),
    "europe-west2": ("London", "Europe", 51.5074, -0.1278),
    "europe-west3": ("Frankfurt", "Europe", 50.1109, 8.6821),
    "europe-west4": ("Eemshaven", "Europe", 53.4357, 6.7865),
    "europe-west6": ("Zurich", "Europe", 47.3769, 8.5417),
    "europe-west8": ("Milan", "Europe", 45.4642, 9.1900),
    "europe-west9": ("Paris", "Europe", 48.8566, 2.3522),
    "europe-west10": ("Berlin", "Europe", 52.5200, 13.4050),
    "europe-west12": ("Turin", "Europe", 45.0703, 7.6869),
    "europe-north1": ("Finland", "Europe", 60.5693, 27.1878),
    "europe-north2": ("Stockholm", "Europe", 59.3293, 18.0686),
    "europe-central2": ("Warsaw", "Europe", 52.2297, 21.0122),
    "europe-southwest1": ("Madrid", "Europe", 40.4168, -3.7038),
    # Asia Pacific
    "asia-east1": ("Taiwan", "Asia Pacific", 24.0815, 120.5383),
    "asia-east2": ("Hong Kong", "Asia Pacific", 22.3193, 114.1694),
    "asia-northeast1": ("Tokyo", "Asia Pacific", 35.6762, 139.6503),
    "asia-northeast2": ("Osaka", "Asia Pacific", 34.6937, 135.5023),
    "asia-northeast3": ("Seoul", "Asia Pacific", 37.5665, 126.9780),
    "asia-south1": ("Mumbai", "Asia Pacific", 19.0760, 72.8777),
    "asia-south2": ("Delhi", "Asia Pacific", 28.6139, 77.2090),
    "asia-southeast1": ("Singapore", "Asia Pacific", 1.3521, 103.8198),
    "asia-southeast2": ("Jakarta", "Asia Pacific", -6.2088, 106.8456),
    "asia-southeast3": ("Bangkok", "Asia Pacific", 13.7563, 100.5018),
    "australia-southeast1": ("Sydney", "Asia Pacific", -33.8688, 151.2093),
    "australia-southeast2": ("Melbourne", "Asia Pacific", -37.8136, 144.9631),
    # Middle East & Africa
    "me-central1": ("Doha", "Middle East & Africa", 25.2854, 51.5310),
    "me-central2": ("Dammam", "Middle East & Africa", 26.4207, 50.0888),
    "me-west1": ("Tel Aviv", "Middle East & Africa", 32.0853, 34.7818),
    "africa-south1": ("Johannesburg", "Middle East & Africa", -26.2041, 28.0473),
}


def haversine_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculates great-circle distance between two points on Earth in kilometers."""
    r = 6371.0  # Earth radius in km
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = math.sin(delta_phi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2.0) ** 2
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return r * c


def _generate_percentile_metrics(base_latency: float, ops_count: int) -> LatencyMetrics:
    """Generates statistically realistic percentiles around a base median latency."""
    p50 = round(base_latency, 2)
    p90 = round(base_latency * random.uniform(1.20, 1.35), 2)
    p95 = round(base_latency * random.uniform(1.35, 1.55), 2)
    p99 = round(base_latency * random.uniform(1.65, 2.10), 2)
    min_lat = round(max(0.8, base_latency * random.uniform(0.75, 0.88)), 2)
    max_lat = round(p99 * random.uniform(1.2, 1.6), 2)
    avg_lat = round((p50 * 0.5) + (p90 * 0.3) + (p95 * 0.15) + (p99 * 0.05), 2)

    return LatencyMetrics(
        ops_count=ops_count,
        p50=p50,
        p90=p90,
        p95=p95,
        p99=p99,
        min=min_lat,
        max=max_lat,
        avg=avg_lat,
    )


def _finish_step(
    step: Optional[BenchmarkStep],
    status: StepStatus,
    error_message: Optional[str] = None,
):
    """Marks a benchmark step completed or failed and computes duration_seconds."""
    if not step:
        return
    step.status = status
    step.completed_at = datetime.now(timezone.utc).isoformat()
    if error_message:
        step.error_message = error_message
    if step.started_at:
        try:
            st = datetime.fromisoformat(step.started_at)
            ct = datetime.fromisoformat(step.completed_at)
            step.duration_seconds = max(0.0, round((ct - st).total_seconds(), 2))
        except Exception:
            step.duration_seconds = 0.0


class BenchmarkService:
    """Service managing Cloud Spanner latency benchmark campaigns across multiple client regions."""

    def __init__(self):
        self.spanner_service = SpannerService()
        self._data_dir = _resolve_data_dir()
        self._benchmarks_file = self._data_dir / "benchmarks.json"
        self._campaigns: Dict[str, BenchmarkCampaignStatus] = {}
        self._tasks: Dict[str, asyncio.Task] = {}
        self._load_from_disk()

    def _load_from_disk(self):
        """Loads saved benchmark campaigns from disk JSON file if present."""
        if self._benchmarks_file.exists():
            try:
                with open(self._benchmarks_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    for item in data:
                        status = BenchmarkCampaignStatus(**item)
                        # Reset in-flight states from prior runs
                        if status.status in (
                            CampaignState.RUNNING,
                            CampaignState.PROVISIONING_SPANNER,
                            CampaignState.PROVISIONING_VMS,
                        ):
                            status.status = CampaignState.STOPPED
                            status.message = "Run was stopped on server restart."
                        # Normalize legacy step description
                        for step in (status.steps or []):
                            if step.id == "execute_workload":
                                step.description = "Running load test"
                            elif step.id == "setup_clients":
                                step.description = "Provision load test runner across client locations"
                        # Populate replica_type on region_results if missing from older saved json
                        if status.spanner_config and status.region_results:
                            cfg_detail = self.spanner_service.get_config_detail(
                                status.spanner_config, leader_region=status.leader_region
                            )
                            if cfg_detail:
                                for reg_id, reg_res in status.region_results.items():
                                    if not reg_res.replica_type:
                                        for rep in cfg_detail.replicas:
                                            if rep.region == reg_id:
                                                if (
                                                    rep.is_leader
                                                    or rep.replica_type == ReplicaType.LEADER
                                                    or reg_id == status.leader_region
                                                ):
                                                    reg_res.replica_type = "Leader Region"
                                                elif rep.replica_type == ReplicaType.RW_REPLICA:
                                                    reg_res.replica_type = "R/W Replica Region"
                                                elif rep.replica_type == ReplicaType.WITNESS or rep.is_witness:
                                                    reg_res.replica_type = "Witness Region"
                                                elif (
                                                    rep.replica_type
                                                    in (ReplicaType.READ_ONLY, ReplicaType.OPTIONAL_READ_ONLY)
                                                    or rep.is_read_only
                                                ):
                                                    reg_res.replica_type = "Read Only Region"
                                                break
                        self._campaigns[status.campaign_id] = status
            except Exception as e:
                print(f"⚠️ Error loading {self._benchmarks_file}: {e}")

    def _save_to_disk(self):
        """Persists all benchmark campaigns to disk JSON file."""
        try:
            self._data_dir.mkdir(parents=True, exist_ok=True)
            with open(self._benchmarks_file, "w", encoding="utf-8") as f:
                raw = [status.model_dump() for status in self._campaigns.values()]
                json.dump(raw, f, indent=2)
        except Exception as e:
            print(f"⚠️ Error saving {self._benchmarks_file}: {e}")

    def _compute_region_results(
        self,
        spanner_config: str,
        client_regions: List[str],
        custom_leader: Optional[str] = None,
        optional_replicas: Optional[List[str]] = None,
    ) -> Tuple[str, Dict[str, RegionBenchmarkResult]]:
        """Calculates distance to leader, replica presence, and builds placeholder results."""
        config_detail = self.spanner_service.get_config_detail(spanner_config, leader_region=custom_leader)
        leader_region = config_detail.leader_region if config_detail else (custom_leader or spanner_config)

        leader_coords = (41.2619, -95.8608)
        if config_detail:
            lc = next(((r.lat, r.lon) for r in config_detail.replicas if r.is_leader), None)
            if not lc:
                lc = next(((r.lat, r.lon) for r in config_detail.replicas if r.region == leader_region), None)
            if lc:
                leader_coords = lc

        opt_set = set(optional_replicas or [])
        region_results: Dict[str, RegionBenchmarkResult] = {}
        for region_id in client_regions:
            region_meta = CLIENT_REGIONS_CATALOG.get(region_id, (region_id, "Unknown", 41.2619, -95.8608))
            region_name = region_meta[0]

            has_replica = False
            replica_type = None
            if config_detail:
                for rep in config_detail.replicas:
                    if rep.region == region_id:
                        if (
                            rep.is_leader
                            or rep.replica_type == ReplicaType.LEADER
                            or region_id == leader_region
                        ):
                            replica_type = "Leader Region"
                            has_replica = True
                        elif rep.replica_type == ReplicaType.RW_REPLICA:
                            replica_type = "R/W Replica Region"
                            has_replica = True
                        elif rep.replica_type == ReplicaType.WITNESS or rep.is_witness:
                            replica_type = "Witness Region"
                            has_replica = False
                        elif rep.replica_type == ReplicaType.OPTIONAL_READ_ONLY:
                            if rep.region in opt_set:
                                replica_type = "Optional Read Only (Provisioned)"
                                has_replica = True
                            else:
                                replica_type = "Optional Read Only (Unprovisioned)"
                                has_replica = False
                        elif rep.replica_type == ReplicaType.READ_ONLY:
                            replica_type = "Read Only Region"
                            has_replica = True
                        break

            if region_id == leader_region:
                dist_to_leader = 0.0
            else:
                dist_to_leader = round(
                    haversine_distance_km(region_meta[2], region_meta[3], leader_coords[0], leader_coords[1]),
                    1,
                )

            region_results[region_id] = RegionBenchmarkResult(
                region=region_id,
                region_name=region_name,
                distance_to_leader_km=dist_to_leader,
                has_local_replica=has_replica,
                replica_type=replica_type,
                direct_access=True,
                status="pending",
                progress_pct=0,
            )
        return leader_region, region_results

    def list_client_regions(self) -> List[ClientRegionInfo]:
        """Returns list of supported GCE client benchmark regions sorted by continent and name."""
        results = []
        for region_id, (name, continent, lat, lon) in CLIENT_REGIONS_CATALOG.items():
            results.append(
                ClientRegionInfo(
                    region=region_id,
                    name=name,
                    continent=continent,
                    latitude=lat,
                    longitude=lon,
                )
            )
        results.sort(key=lambda r: (r.continent, r.region))
        return results

    def list_campaigns(self) -> List[BenchmarkCampaignStatus]:
        """Returns all benchmark campaigns ordered by creation date descending."""
        campaigns = list(self._campaigns.values())
        campaigns.sort(key=lambda c: c.created_at, reverse=True)
        return campaigns

    def get_campaign(self, campaign_id: str) -> Optional[BenchmarkCampaignStatus]:
        """Retrieves status of a specific campaign."""
        return self._campaigns.get(campaign_id)

    def _log(self, campaign: BenchmarkCampaignStatus, level: str, stage: str, message: str):
        entry = BenchmarkLogEntry(
            timestamp=datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3],
            level=level,
            stage=stage,
            message=message,
        )
        campaign.logs.append(entry)
        self._save_to_disk()

    def create_campaign(self, config: BenchmarkCampaignConfig) -> BenchmarkCampaignStatus:
        """Creates a new benchmark test record without running it immediately."""
        campaign_id = f"cmp-{uuid.uuid4().hex[:8]}"
        now = datetime.now(timezone.utc).isoformat()
        clean_opt_replicas = [r for r in config.optional_replicas if r]
        leader_region, region_results = self._compute_region_results(
            config.spanner_config,
            config.client_regions,
            custom_leader=config.leader_region,
            optional_replicas=clean_opt_replicas,
        )

        name = config.name.strip() if config.name else f"Benchmark ({config.spanner_config})"

        status = BenchmarkCampaignStatus(
            campaign_id=campaign_id,
            name=name,
            description=config.description or "",
            status=CampaignState.DRAFT,
            progress_pct=0,
            project_id=config.project_id,
            spanner_instance_id=None,
            spanner_database_id=None,
            spanner_config=config.spanner_config,
            leader_region=leader_region,
            client_regions=config.client_regions,
            optional_replicas=clean_opt_replicas,
            operations=config.operations,
            staleness_seconds=config.staleness_seconds,
            message="Configured and ready to run.",
            execution_mode=config.execution_mode,
            auto_teardown=config.auto_teardown,
            staging_bucket=config.staging_bucket,
            steps=_init_default_steps(),
            logs=[],
            region_results=region_results,
            created_at=now,
        )
        self._campaigns[campaign_id] = status
        self._log(status, "INFO", "lifecycle", f"Benchmark '{name}' created in mode '{config.execution_mode}'.")
        self._save_to_disk()
        return status

    def update_campaign(
        self, campaign_id: str, update: BenchmarkCampaignUpdate
    ) -> Optional[BenchmarkCampaignStatus]:
        """Updates benchmark test configuration parameters."""
        campaign = self._campaigns.get(campaign_id)
        if not campaign:
            return None

        # Stop active task if running
        if campaign_id in self._tasks and not self._tasks[campaign_id].done():
            self._tasks[campaign_id].cancel()

        if update.name is not None:
            campaign.name = update.name
        if update.description is not None:
            campaign.description = update.description
        if update.project_id is not None:
            campaign.project_id = update.project_id
        if update.execution_mode is not None:
            campaign.execution_mode = update.execution_mode
        if update.operations is not None:
            campaign.operations = update.operations
        if update.staleness_seconds is not None:
            campaign.staleness_seconds = update.staleness_seconds
        if update.auto_teardown is not None:
            campaign.auto_teardown = update.auto_teardown
        if update.staging_bucket is not None:
            campaign.staging_bucket = update.staging_bucket

        recompute = False
        if update.optional_replicas is not None:
            campaign.optional_replicas = [r for r in update.optional_replicas if r]
            recompute = True
        if update.spanner_config is not None and update.spanner_config != campaign.spanner_config:
            campaign.spanner_config = update.spanner_config
            recompute = True
        if update.leader_region is not None and update.leader_region != campaign.leader_region:
            campaign.leader_region = update.leader_region
            recompute = True
        if update.client_regions is not None:
            campaign.client_regions = update.client_regions
            recompute = True

        if recompute:
            leader_region, region_results = self._compute_region_results(
                campaign.spanner_config,
                campaign.client_regions,
                custom_leader=campaign.leader_region,
                optional_replicas=campaign.optional_replicas,
            )
            campaign.leader_region = leader_region
            campaign.region_results = region_results

        self._save_to_disk()
        return campaign

    async def _teardown_campaign_cloud_resources(
        self, campaign: BenchmarkCampaignStatus, gcloud_bin: str
    ) -> None:
        """
        Asynchronously tears down all cloud resources (GCE runner VMs and Spanner instance)
        associated with a campaign in parallel. Discovers VMs both from campaign.gce_instances
        and via GCP labels to ensure no orphaned instances remain.
        """
        if campaign.execution_mode != "live" or not (gcloud_bin and os.path.exists(gcloud_bin)):
            return

        delete_tasks = []

        # 1. Collect GCE VMs to delete
        vms_to_delete: Dict[str, str] = {}  # vm_name -> zone

        # From recorded instances dictionary
        if hasattr(campaign, "gce_instances") and campaign.gce_instances:
            for entry in campaign.gce_instances.values():
                if ":" in entry:
                    name, zone = entry.split(":", 1)
                    vms_to_delete[name] = zone
                elif entry:
                    vms_to_delete[entry] = ""

        # Query GCP for any instances labeled for this specific campaign (handles interrupted provisioning)
        try:
            list_proc = await asyncio.create_subprocess_exec(
                gcloud_bin, "compute", "instances", "list",
                f"--project={campaign.project_id}",
                f"--filter=labels.dbexplorer-campaign={campaign.campaign_id}",
                "--format=value(name,zone)",
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            out, _ = await list_proc.communicate()
            if list_proc.returncode == 0 and out.strip():
                for line in out.decode("utf-8", errors="replace").strip().splitlines():
                    parts = line.strip().split()
                    if len(parts) >= 2:
                        vms_to_delete[parts[0]] = parts[1]
                    elif len(parts) == 1:
                        vms_to_delete.setdefault(parts[0], "")
        except Exception as ex:
            logger.warning(f"Error querying GCP for labeled VMs: {ex}")

        # Build parallel async VM deletion coroutines
        for vm_name, zone in vms_to_delete.items():
            cmd = [
                gcloud_bin, "compute", "instances", "delete", vm_name,
                f"--project={campaign.project_id}", "--quiet"
            ]
            if zone:
                cmd.append(f"--zone={zone}")

            async def _del_vm(c_cmd=cmd, name=vm_name):
                try:
                    p = await asyncio.create_subprocess_exec(
                        *c_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                    )
                    await p.communicate()
                    if p.returncode == 0:
                        logger.info(f"Successfully deleted VM {name}")
                    else:
                        logger.warning(f"Failed to delete VM {name} (exit code {p.returncode})")
                except Exception as e:
                    logger.warning(f"Exception deleting VM {name}: {e}")

            delete_tasks.append(_del_vm())

        # 2. Add Spanner instance deletion coroutine
        if campaign.spanner_instance_id:
            inst_id = campaign.spanner_instance_id

            async def _del_spanner(i_id=inst_id):
                try:
                    p = await asyncio.create_subprocess_exec(
                        gcloud_bin, "spanner", "instances", "delete", i_id,
                        f"--project={campaign.project_id}", "--quiet",
                        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                    )
                    await p.communicate()
                    if p.returncode == 0:
                        logger.info(f"Successfully deleted Spanner instance {i_id}")
                    else:
                        logger.warning(f"Failed to delete Spanner instance {i_id} (exit code {p.returncode})")
                except Exception as e:
                    logger.warning(f"Exception deleting Spanner instance {i_id}: {e}")

            delete_tasks.append(_del_spanner())

        # 3. Execute all deletions in parallel
        if delete_tasks:
            self._log(campaign, "INFO", "teardown", f"Tearing down {len(vms_to_delete)} VMs and Spanner instance in parallel...")
            await asyncio.gather(*delete_tasks, return_exceptions=True)
            self._log(campaign, "SUCCESS", "teardown", "Cloud resources teardown complete.")

        # 4. If custom instance configuration was created for optional read replicas, delete it after instance is deleted
        if campaign.optional_replicas:
            custom_config_id = f"custom-{campaign.spanner_config}-{campaign.campaign_id[:8]}"
            try:
                cfg_proc = await asyncio.create_subprocess_exec(
                    gcloud_bin, "spanner", "instance-configs", "delete", custom_config_id,
                    f"--project={campaign.project_id}", "--quiet",
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                )
                await cfg_proc.communicate()
                if cfg_proc.returncode == 0:
                    logger.info(f"Successfully deleted custom instance config {custom_config_id}")
            except Exception as e:
                logger.warning(f"Exception deleting custom instance config {custom_config_id}: {e}")

    async def delete_campaign(self, campaign_id: str) -> bool:
        """Deletes a benchmark campaign and tears down any active resources."""
        await self.stop_campaign(campaign_id)
        if campaign_id in self._campaigns:
            campaign = self._campaigns[campaign_id]
            campaign.status = CampaignState.DELETING
            campaign.message = "Deleting cloud resources and campaign record..."
            self._save_to_disk()

            gcloud_bin = find_gcloud_binary() or "gcloud"
            await self._teardown_campaign_cloud_resources(campaign, gcloud_bin)

            del self._campaigns[campaign_id]
            self._save_to_disk()
            return True
        return False

    async def start_campaign(self, config: BenchmarkCampaignConfig) -> BenchmarkCampaignStatus:
        """Creates and starts a benchmark campaign immediately."""
        status = self.create_campaign(config)
        await self.run_existing_campaign(status.campaign_id)
        return status

    async def run_existing_campaign(self, campaign_id: str) -> Optional[BenchmarkCampaignStatus]:
        """Launches execution on an existing benchmark test."""
        campaign = self._campaigns.get(campaign_id)
        if not campaign:
            return None

        # Stop any running task for this campaign
        if campaign_id in self._tasks and not self._tasks[campaign_id].done():
            self._tasks[campaign_id].cancel()

        # Re-initialize region result states using custom leader and optional replicas if configured
        leader_region, region_results = self._compute_region_results(
            campaign.spanner_config,
            campaign.client_regions,
            custom_leader=campaign.leader_region,
            optional_replicas=campaign.optional_replicas,
        )
        campaign.leader_region = leader_region
        campaign.region_results = region_results
        campaign.steps = _init_default_steps()
        campaign.status = CampaignState.PREFLIGHT_CHECK
        campaign.progress_pct = 5
        campaign.spanner_instance_id = f"spanner-bench-{uuid.uuid4().hex[:6]}"
        campaign.spanner_database_id = "benchmark-db"
        campaign.message = f"Starting benchmark on {campaign.spanner_config}..."
        campaign.completed_at = None
        self._log(campaign, "INFO", "lifecycle", f"Initiating execution run (mode: {campaign.execution_mode}, target: {campaign.spanner_config})...")
        self._save_to_disk()

        task = asyncio.create_task(self._run_campaign_orchestrator(campaign_id))
        self._tasks[campaign_id] = task
        return campaign

    async def stop_campaign(self, campaign_id: str) -> bool:
        """Stops an in-flight campaign."""
        if campaign_id in self._tasks and not self._tasks[campaign_id].done():
            self._tasks[campaign_id].cancel()
        if campaign_id in self._campaigns:
            campaign = self._campaigns[campaign_id]
            campaign.status = CampaignState.STOPPED
            campaign.message = "Benchmark campaign was stopped by user."
            campaign.completed_at = datetime.now(timezone.utc).isoformat()
            self._log(campaign, "WARN", "lifecycle", "Benchmark was stopped by user.")
            for r in campaign.region_results.values():
                if r.status in ("pending", "provisioning", "running"):
                    r.status = "stopped"
            self._save_to_disk()
            return True
        return False

    async def cleanup_campaign(self, campaign_id: str) -> bool:
        """Deletes all resources (VMs and Spanner instance) while preserving campaign definition and results."""
        await self.stop_campaign(campaign_id)
        if campaign_id in self._campaigns:
            campaign = self._campaigns[campaign_id]
            gcloud_bin = find_gcloud_binary() or "gcloud"
            await self._teardown_campaign_cloud_resources(campaign, gcloud_bin)

            campaign.gce_instances = {}
            campaign.spanner_instance_id = None
            campaign.spanner_database_id = None
            if campaign.status in (
                CampaignState.RUNNING,
                CampaignState.PROVISIONING_SPANNER,
                CampaignState.PROVISIONING_VMS,
                CampaignState.PREFLIGHT_CHECK,
            ):
                campaign.status = CampaignState.STOPPED
                campaign.message = "Cloud resources torn down manually by user."
            self._save_to_disk()
            return True
        return False

    async def cleanup_all_resources(self) -> Dict[str, Any]:
        """Tears down all provisioned resources across all campaigns, including GCE VMs and Spanner instances."""
        cleaned_count = 0
        gcloud_bin = find_gcloud_binary() or "gcloud"
        active_project = None

        # 1. Teardown active campaigns
        teardown_tasks = []
        for campaign_id, campaign in list(self._campaigns.items()):
            if campaign_id in self._tasks and not self._tasks[campaign_id].done():
                self._tasks[campaign_id].cancel()
            if campaign.project_id:
                active_project = campaign.project_id
            if campaign.spanner_instance_id or campaign.gce_instances or campaign.status in (
                CampaignState.RUNNING,
                CampaignState.PROVISIONING_SPANNER,
                CampaignState.PROVISIONING_VMS,
                CampaignState.PREFLIGHT_CHECK,
            ):
                cleaned_count += 1
                if campaign.execution_mode == "live" and os.path.exists(gcloud_bin):
                    teardown_tasks.append(self._teardown_campaign_cloud_resources(campaign, gcloud_bin))

            campaign.gce_instances = {}
            campaign.spanner_instance_id = None
            campaign.spanner_database_id = None
            if campaign.status in (
                CampaignState.RUNNING,
                CampaignState.PROVISIONING_SPANNER,
                CampaignState.PROVISIONING_VMS,
                CampaignState.PREFLIGHT_CHECK,
            ):
                campaign.status = CampaignState.STOPPED
                campaign.message = "All cloud resources purged."

        if teardown_tasks:
            await asyncio.gather(*teardown_tasks, return_exceptions=True)

        # 2. Also sweep any orphaned benchmark VMs labeled with dbexplorer-benchmark=true
        if active_project and os.path.exists(gcloud_bin):
            try:
                list_proc = await asyncio.create_subprocess_exec(
                    gcloud_bin, "compute", "instances", "list",
                    f"--project={active_project}",
                    "--filter=labels.dbexplorer-benchmark=true",
                    "--format=value(name,zone)",
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                )
                out, _ = await list_proc.communicate()
                if list_proc.returncode == 0 and out.strip():
                    sweep_tasks = []
                    for line in out.decode("utf-8", errors="replace").strip().splitlines():
                        parts = line.strip().split()
                        if len(parts) >= 2:
                            vm_name, zone = parts[0], parts[1]
                            sweep_tasks.append(
                                asyncio.create_subprocess_exec(
                                    gcloud_bin, "compute", "instances", "delete", vm_name,
                                    f"--zone={zone}", f"--project={active_project}", "--quiet",
                                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                                )
                            )
                    if sweep_tasks:
                        procs = await asyncio.gather(*sweep_tasks, return_exceptions=True)
                        for p in procs:
                            if hasattr(p, "communicate"):
                                await p.communicate()
            except Exception as ex:
                logger.warning(f"Sweep of orphaned GCE VMs encountered error: {ex}")

        self._save_to_disk()
        return {
            "status": "success",
            "cleaned_campaigns_count": cleaned_count,
            "message": f"Purged resources across {cleaned_count} benchmark campaigns and swept GCE VMs.",
        }

    async def _ensure_staging_jar(
        self, project_id: str, gcloud_bin: str, campaign: BenchmarkCampaignStatus
    ) -> Optional[str]:
        """
        Ensures the benchmark runner shaded JAR is uploaded to a GCS bucket accessible via Private Google Access.
        Returns the gs:// URI or None on failure.
        """
        jar_path = BACKEND_DIR.parent / "benchmark" / "target" / "spanner-direct-access-benchmark-1.0.0.jar"
        if not jar_path.exists():
            mvn_bin = find_mvn_binary()
            if mvn_bin:
                self._log(campaign, "INFO", "clients", f"Runner JAR '{jar_path.name}' not found locally. Auto-compiling with Maven ({mvn_bin})...")
                pom_path = BACKEND_DIR.parent / "benchmark" / "pom.xml"
                if pom_path.exists():
                    try:
                        build_proc = await asyncio.create_subprocess_exec(
                            mvn_bin, "clean", "package", "-DskipTests",
                            cwd=str(BACKEND_DIR.parent / "benchmark"),
                            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                        )
                        b_out, b_err = await build_proc.communicate()
                        if build_proc.returncode == 0 and jar_path.exists():
                            self._log(campaign, "SUCCESS", "clients", f"Successfully built runner JAR {jar_path.name} ({os.path.getsize(jar_path)} bytes).")
                        else:
                            err_snippet = (b_err or b_out).decode("utf-8", errors="replace")[-500:].strip()
                            self._log(campaign, "ERROR", "clients", f"Maven compilation failed: {err_snippet}")
                    except Exception as ex:
                        self._log(campaign, "ERROR", "clients", f"Failed to execute Maven build: {ex}")

        if not jar_path.exists():
            err_msg = (
                f"Runner JAR '{jar_path.name}' is missing at {jar_path}. "
                "Please run 'mvn clean package -DskipTests' in the benchmark directory."
            )
            self._log(campaign, "ERROR", "clients", err_msg)
            campaign.message = err_msg
            return None

        # Resolve target staging bucket hierarchy:
        # 1. User-specified staging_bucket on campaign
        # 2. Existing default bucket: gs://{project_id}-dbexplorer-staging
        # 3. Existing project bucket (e.g. cloudbuild or staging bucket) to prevent creating new buckets
        # 4. Create gs://{project_id}-dbexplorer-staging as fallback

        staging_bucket: Optional[str] = None
        if campaign.staging_bucket:
            candidate = campaign.staging_bucket.strip().replace("gs://", "").rstrip("/")
            if re.match(r"^[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]$", candidate):
                staging_bucket = candidate
                self._log(campaign, "INFO", "clients", f"Using user-specified staging bucket gs://{staging_bucket}")
            else:
                self._log(campaign, "WARN", "clients", f"Invalid staging bucket name '{campaign.staging_bucket}', falling back to auto-detection.")

        default_bucket = f"{project_id}-dbexplorer-staging"

        # 1. Check if default bucket already exists
        if not staging_bucket:
            desc_proc = await asyncio.create_subprocess_exec(
                gcloud_bin, "storage", "buckets", "describe", f"gs://{default_bucket}",
                f"--project={project_id}",
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            await desc_proc.communicate()
            if desc_proc.returncode == 0:
                staging_bucket = default_bucket
                self._log(campaign, "INFO", "clients", f"Reusing existing staging bucket gs://{staging_bucket}")

        # 2. Search for existing project buckets to avoid unnecessary bucket creation
        if not staging_bucket:
            list_b_proc = await asyncio.create_subprocess_exec(
                gcloud_bin, "storage", "buckets", "list", f"--project={project_id}", "--format=value(name)",
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            b_out, _ = await list_b_proc.communicate()
            b_names = [b.strip() for b in b_out.decode("utf-8", errors="replace").splitlines() if b.strip()]
            if b_names:
                preferred = next((b for b in b_names if "staging" in b.lower()), None)
                if not preferred:
                    preferred = next((b for b in b_names if "cloudbuild" in b.lower()), b_names[0])
                staging_bucket = preferred
                self._log(campaign, "INFO", "clients", f"Reusing existing project bucket gs://{staging_bucket} for runner staging.")

        # 3. Create default staging bucket as last resort
        if not staging_bucket:
            self._log(campaign, "INFO", "clients", f"Creating staging bucket gs://{default_bucket}...")
            create_b_proc = await asyncio.create_subprocess_exec(
                gcloud_bin, "storage", "buckets", "create", f"gs://{default_bucket}",
                f"--project={project_id}", "--location=europe-west1", "--uniform-bucket-level-access",
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            c_out, c_err = await create_b_proc.communicate()
            if create_b_proc.returncode == 0:
                staging_bucket = default_bucket
                self._log(campaign, "SUCCESS", "clients", f"Created staging bucket gs://{staging_bucket}.")
            else:
                err_msg = c_err.decode("utf-8", errors="replace").strip()
                self._log(campaign, "ERROR", "clients", f"Failed to create staging bucket gs://{default_bucket}: {err_msg}")
                campaign.message = f"Failed to create staging bucket gs://{default_bucket}: {err_msg}"
                return None

        campaign.staging_bucket = staging_bucket
        self._save_to_disk()

        gcs_target = f"gs://{staging_bucket}/dbexplorer/{jar_path.name}"

        # Check if jar is already staged and matches local size
        check_obj_proc = await asyncio.create_subprocess_exec(
            gcloud_bin, "storage", "objects", "describe", gcs_target, "--format=value(size)",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        describe_out, _ = await check_obj_proc.communicate()
        remote_size = describe_out.decode("utf-8", errors="replace").strip()
        local_size = str(os.path.getsize(jar_path))

        needs_upload = (check_obj_proc.returncode != 0) or (remote_size != local_size)

        if needs_upload:
            self._log(campaign, "INFO", "clients", f"Uploading {jar_path.name} ({local_size} bytes) to {gcs_target}...")
            cp_proc = await asyncio.create_subprocess_exec(
                gcloud_bin, "storage", "cp", str(jar_path), gcs_target,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            cp_out, cp_err = await cp_proc.communicate()
            if cp_proc.returncode != 0:
                err_msg = cp_err.decode("utf-8", errors="replace").strip()
                self._log(campaign, "ERROR", "clients", f"Failed to upload runner JAR: {err_msg}")
                campaign.message = f"Failed to upload runner JAR to {gcs_target}: {err_msg}"
                return None
            self._log(campaign, "SUCCESS", "clients", f"Staged runner JAR to {gcs_target}.")
        else:
            self._log(campaign, "INFO", "clients", f"Runner JAR already staged at {gcs_target} ({remote_size} bytes, matches local).")

        return gcs_target

    async def _resolve_zone_for_region(self, region: str, project_id: str, gcloud_bin: str) -> str:
        """Finds an active (UP) zone in the target region."""
        try:
            proc = await asyncio.create_subprocess_exec(
                gcloud_bin, "compute", "zones", "list",
                f"--filter=region={region} AND status=UP",
                "--format=value(name)",
                f"--project={project_id}",
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            out, _ = await proc.communicate()
            if proc.returncode == 0:
                zones = [z.strip() for z in out.decode("utf-8", errors="replace").splitlines() if z.strip()]
                if zones:
                    return zones[0]
        except Exception:
            pass
        return f"{region}-b"

    def _emergency_cleanup_resources(
        self,
        campaign: BenchmarkCampaignStatus,
        gcloud_bin: str,
        instance_id: Optional[str] = None,
    ):
        """Emergency cleanup of provisioned GCP resources (VMs and Spanner instance) after an error or cancellation."""
        if not campaign.auto_teardown or campaign.execution_mode != "live":
            return

        has_gcloud = bool(gcloud_bin and os.path.exists(gcloud_bin))

        if campaign.gce_instances:
            self._log(campaign, "INFO", "teardown", f"Emergency cleanup: deleting {len(campaign.gce_instances)} GCE VMs...")
            if has_gcloud:
                for reg, entry in list(campaign.gce_instances.items()):
                    try:
                        vm_name, zone = entry.split(":")
                        subprocess.run(
                            [
                                gcloud_bin, "compute", "instances", "delete", vm_name,
                                f"--zone={zone}", f"--project={campaign.project_id}", "--quiet",
                            ],
                            capture_output=True, timeout=30,
                        )
                    except Exception:
                        pass
            campaign.gce_instances = {}

        inst_to_del = instance_id or campaign.spanner_instance_id
        if inst_to_del:
            if has_gcloud:
                try:
                    self._log(campaign, "INFO", "teardown", f"Emergency cleanup: deleting Spanner instance '{inst_to_del}' to avoid ongoing billing...")
                    del_res = subprocess.run(
                        [
                            gcloud_bin, "spanner", "instances", "delete", inst_to_del,
                            f"--project={campaign.project_id}", "--quiet",
                        ],
                        capture_output=True, timeout=60,
                    )
                    if del_res.returncode == 0:
                        self._log(campaign, "SUCCESS", "teardown", f"Emergency cleanup deleted Spanner instance '{inst_to_del}'.")
                    else:
                        err = del_res.stderr.decode("utf-8", errors="replace").strip()
                        self._log(campaign, "WARN", "teardown", f"Could not delete Spanner instance '{inst_to_del}': {err}")
                except Exception as ex:
                    self._log(campaign, "WARN", "teardown", f"Exception during Spanner cleanup: {ex}")
            campaign.spanner_instance_id = None
            campaign.spanner_database_id = None

    async def _provision_client_vm(
        self,
        region: str,
        campaign: BenchmarkCampaignStatus,
        gcloud_bin: str,
        jar_gcs_path: str,
        instance_id: str,
        db_id: str,
        startup_script_path: Path,
    ) -> Tuple[str, str, bool, str]:
        """Provisions an ephemeral GCE VM (e2-standard-2) in an active zone within region."""
        zone = await self._resolve_zone_for_region(region, campaign.project_id, gcloud_bin)
        short_cid = campaign.campaign_id[:6]
        vm_name = f"dbbench-{short_cid}-{region}"

        self._log(campaign, "INFO", "clients", f"Provisioning client VM '{vm_name}' in zone '{zone}' (region: {region})...")
        image_family = os.environ.get("DBEXPLORER_GCE_IMAGE_FAMILY", "ubuntu-2204-lts")
        image_project = os.environ.get("DBEXPLORER_GCE_IMAGE_PROJECT", "ubuntu-os-cloud")

        cmd = [
            gcloud_bin, "compute", "instances", "create", vm_name,
            f"--project={campaign.project_id}",
            f"--zone={zone}",
            "--machine-type=e2-standard-2",
            f"--image-family={image_family}",
            f"--image-project={image_project}",
            "--subnet=default",
            "--scopes=cloud-platform",
            f"--labels=dbexplorer-benchmark=true,dbexplorer-campaign={campaign.campaign_id}",
            f"--metadata=enable-guest-attributes=TRUE,enable-osconfig=FALSE,spanner_project_id={campaign.project_id},spanner_instance_id={instance_id},spanner_database_id={db_id},benchmark_operations={campaign.operations},benchmark_staleness={campaign.staleness_seconds},spanner_leader_region={campaign.leader_region or ''},jar_gcs_path={jar_gcs_path}",
            f"--metadata-from-file=startup-script={str(startup_script_path)}",
            "--max-run-duration=30m",
            "--instance-termination-action=DELETE",
            "--quiet",
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
        )
        out, _ = await proc.communicate()
        out_text = out.decode("utf-8", errors="replace").strip()
        if proc.returncode == 0:
            self._log(campaign, "SUCCESS", "clients", f"Client VM '{vm_name}' provisioned successfully in {zone}.")
            return region, f"{vm_name}:{zone}", True, ""
        else:
            self._log(campaign, "ERROR", "clients", f"Failed to provision client VM '{vm_name}' in {zone}: {out_text}")
            return region, f"{vm_name}:{zone}", False, out_text

    async def _poll_vm_results(
        self,
        region: str,
        vm_entry: str,
        campaign: BenchmarkCampaignStatus,
        gcloud_bin: str,
        timeout_seconds: int = 360,
    ) -> Tuple[str, Optional[dict], Optional[str]]:
        """
        Polls GCE Guest Attributes for the benchmark output JSON.
        Falls back to serial port output on VM stop/termination.
        """
        vm_name, zone = vm_entry.split(":")
        start_time = asyncio.get_event_loop().time()
        poll_count = 0

        while (asyncio.get_event_loop().time() - start_time) < timeout_seconds:
            poll_count += 1

            # 1. Check Guest Attributes
            proc = await asyncio.create_subprocess_exec(
                gcloud_bin, "compute", "instances", "get-guest-attributes", vm_name,
                f"--zone={zone}",
                "--query-path=dbexplorer/results",
                f"--project={campaign.project_id}",
                "--format=value(value)",
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            out, _ = await proc.communicate()
            val_text = out.decode("utf-8", errors="replace").strip()

            if val_text:
                try:
                    data = json.loads(val_text)
                    if "error" in data:
                        return region, None, data["error"]
                    if "writes" in data or "strongReads" in data:
                        return region, data, None
                except Exception:
                    pass

            # 2. Periodically or on VM stop, inspect serial console logs for early completion or fast failure
            check_serial = (poll_count % 3 == 0)
            inst_status = ""
            if check_serial:
                status_proc = await asyncio.create_subprocess_exec(
                    gcloud_bin, "compute", "instances", "describe", vm_name,
                    f"--zone={zone}", f"--project={campaign.project_id}",
                    "--format=value(status)",
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                )
                s_out, _ = await status_proc.communicate()
                inst_status = s_out.decode("utf-8", errors="replace").strip()

            if check_serial or inst_status in ("TERMINATED", "STOPPED"):
                serial_proc = await asyncio.create_subprocess_exec(
                    gcloud_bin, "compute", "instances", "get-serial-port-output", vm_name,
                    f"--zone={zone}", f"--project={campaign.project_id}",
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                )
                ser_out, _ = await serial_proc.communicate()
                ser_text = ser_out.decode("utf-8", errors="replace")

                if "DBEXPLORER_BENCHMARK_OUTPUT_BEGIN" in ser_text:
                    try:
                        chunk = ser_text.split("DBEXPLORER_BENCHMARK_OUTPUT_BEGIN")[1].split("DBEXPLORER_BENCHMARK_OUTPUT_END")[0].strip()
                        data = json.loads(chunk)
                        return region, data, None
                    except Exception:
                        pass

                if "DBEXPLORER_BENCHMARK_ERROR:" in ser_text:
                    err_lines = [l for l in ser_text.splitlines() if "DBEXPLORER_BENCHMARK_ERROR:" in l]
                    err_msg = err_lines[-1].split("DBEXPLORER_BENCHMARK_ERROR:")[1].strip() if err_lines else "Benchmark runner reported an error."
                    return region, None, f"VM runner error: {err_msg}"

                if 'Script "startup-script" failed with error' in ser_text:
                    err_lines = [l for l in ser_text.splitlines() if "startup-script" in l or "failed with error" in l]
                    err_msg = err_lines[-1] if err_lines else "Startup script failed with non-zero exit status."
                    return region, None, f"VM startup script failed: {err_msg}"

                if inst_status in ("TERMINATED", "STOPPED"):
                    err_lines = [l for l in ser_text.splitlines() if "error" in l.lower() or "failed" in l.lower()]
                    err_summary = err_lines[-1] if err_lines else f"VM {vm_name} terminated without reporting results."
                    return region, None, err_summary

            await asyncio.sleep(4.0)

        return region, None, f"Workload timed out after {timeout_seconds}s on VM {vm_name} ({zone})."


    async def _run_campaign_orchestrator(self, campaign_id: str):
        """Asynchronous worker executing the 7 campaign lifecycle transitions with live logs."""
        instance_id: Optional[str] = None
        gcloud_bin = find_gcloud_binary() or "gcloud"
        try:
            campaign = self._campaigns.get(campaign_id)
            if not campaign:
                return
            instance_id = campaign.spanner_instance_id
            config_detail = self.spanner_service.get_config_detail(
                campaign.spanner_config, leader_region=campaign.leader_region
            )

            # Ensure default steps are initialized
            if not campaign.steps or len(campaign.steps) != 7:
                campaign.steps = _init_default_steps()

            # -------------------------------------------------------------
            # Step 1: Pre-flight Verification
            # -------------------------------------------------------------
            step1 = next((s for s in campaign.steps if s.id == "preflight"), None)
            if step1:
                step1.status = StepStatus.RUNNING
                step1.started_at = datetime.now(timezone.utc).isoformat()
            campaign.status = CampaignState.PREFLIGHT_CHECK
            campaign.progress_pct = 5
            campaign.message = f"Running pre-flight verification on project '{campaign.project_id}'..."
            self._log(campaign, "INFO", "preflight", f"Initiating pre-flight verification (mode: {campaign.execution_mode}, project: {campaign.project_id})...")
            self._save_to_disk()

            preflight_service = PreflightService()
            preflight_res = await preflight_service.execute_preflight(
                PreflightCheckRequest(
                    project_id=campaign.project_id,
                    client_regions=campaign.client_regions,
                    spanner_config=campaign.spanner_config,
                    execution_mode=campaign.execution_mode,
                    staging_bucket=campaign.staging_bucket,
                )
            )

            for c in preflight_res.checks:
                lvl = "SUCCESS" if c.status == "passed" else "WARN" if c.status == "warning" else "ERROR"
                self._log(campaign, lvl, "preflight", f"[{c.status.upper()}] {c.name}: {c.message}")
                if c.remediation_command:
                    self._log(campaign, "WARN", "preflight", f"Remediation command: {c.remediation_command}")

            if not preflight_res.all_passed:
                if step1:
                    _finish_step(step1, StepStatus.FAILED, error_message=preflight_res.summary)
                campaign.status = CampaignState.FAILED
                campaign.message = f"Pre-flight verification failed: {preflight_res.summary}"
                self._log(campaign, "ERROR", "preflight", "Benchmark execution halted due to pre-flight failure. See remediation commands above.")
                self._save_to_disk()
                return

            if step1:
                _finish_step(step1, StepStatus.COMPLETED)
            self._log(campaign, "SUCCESS", "preflight", "All pre-flight checks passed successfully.")
            self._save_to_disk()

            # -------------------------------------------------------------
            # Step 2: Cloud Spanner Provisioning
            # -------------------------------------------------------------
            step2 = next((s for s in campaign.steps if s.id == "provision_spanner"), None)
            if step2:
                step2.status = StepStatus.RUNNING
                step2.started_at = datetime.now(timezone.utc).isoformat()
            campaign.status = CampaignState.PROVISIONING_SPANNER
            campaign.progress_pct = 20
            instance_id = campaign.spanner_instance_id or f"spanner-bench-{uuid.uuid4().hex[:6]}"
            campaign.spanner_instance_id = instance_id
            gcp_config = self.spanner_service.get_gcp_instance_config(campaign.spanner_config)
            clean_opt_replicas = [r for r in (campaign.optional_replicas or []) if r]
            if clean_opt_replicas:
                effective_config = f"custom-{campaign.spanner_config}-{campaign.campaign_id[:8]}"
            else:
                effective_config = gcp_config

            campaign.message = f"Provisioning Cloud Spanner 1-node instance '{instance_id}' ({effective_config})..."
            if clean_opt_replicas:
                self._log(campaign, "INFO", "spanner", f"Custom instance configuration '{effective_config}' required for {len(clean_opt_replicas)} optional read-only replica(s): {', '.join(clean_opt_replicas)}.")
            else:
                self._log(campaign, "INFO", "spanner", f"Target Spanner instance '{instance_id}' configuration: {campaign.spanner_config} (GCP config: {gcp_config}), 1 node (1000 processing units).")
            self._save_to_disk()

            if campaign.execution_mode == "live" and os.path.exists(gcloud_bin):
                # If optional replicas requested, create custom instance config first if it doesn't exist
                if clean_opt_replicas:
                    check_cfg_cmd = [gcloud_bin, "spanner", "instance-configs", "describe", effective_config, f"--project={campaign.project_id}", "--format=json"]
                    res_cfg = subprocess.run(check_cfg_cmd, capture_output=True, text=True)
                    if res_cfg.returncode != 0:
                        self._log(campaign, "INFO", "spanner", f"Creating custom instance configuration '{effective_config}' (clone: {gcp_config}) with replicas {clean_opt_replicas}...")
                        create_cfg_cmd = [
                            gcloud_bin, "spanner", "instance-configs", "create", effective_config,
                            f"--clone-config={gcp_config}",
                            f"--display-name=Custom Config ({campaign.spanner_config})",
                            f"--project={campaign.project_id}",
                        ]
                        for reg in clean_opt_replicas:
                            create_cfg_cmd.append(f"--add-replicas=location={reg},type=READ_ONLY")

                        cfg_proc = await asyncio.create_subprocess_exec(
                            *create_cfg_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
                        )
                        cfg_out, _ = await cfg_proc.communicate()
                        cfg_text = cfg_out.decode("utf-8", errors="replace").strip()
                        if cfg_text:
                            self._log(campaign, "INFO", "spanner", cfg_text)
                        if cfg_proc.returncode != 0 and "ALREADY_EXISTS" not in cfg_text:
                            raise RuntimeError(f"Failed to create custom Spanner instance config: {cfg_text}")
                        self._log(campaign, "SUCCESS", "spanner", f"Custom instance configuration '{effective_config}' created.")

                # Check if instance already exists
                check_cmd = [gcloud_bin, "spanner", "instances", "describe", instance_id, f"--project={campaign.project_id}", "--format=json"]
                res = subprocess.run(check_cmd, capture_output=True, text=True)
                if res.returncode != 0:
                    self._log(campaign, "INFO", "spanner", f"Creating Spanner instance: gcloud spanner instances create {instance_id} --config={effective_config} --nodes=1 --project={campaign.project_id}...")
                    create_cmd = [
                        gcloud_bin, "spanner", "instances", "create", instance_id,
                        f"--config={effective_config}",
                        "--nodes=1",
                        "--description=DBExplorer Benchmark Instance",
                        f"--project={campaign.project_id}",
                    ]
                    create_proc = await asyncio.create_subprocess_exec(
                        *create_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
                    )
                    out, _ = await create_proc.communicate()
                    out_text = out.decode("utf-8", errors="replace").strip()
                    if out_text:
                        self._log(campaign, "INFO", "spanner", out_text)
                    if create_proc.returncode != 0:
                        raise RuntimeError(f"Failed to create Spanner instance: {out_text}")
                self._log(campaign, "SUCCESS", "spanner", f"Spanner instance '{instance_id}' is ready.")
            else:
                if clean_opt_replicas:
                    self._log(campaign, "INFO", "spanner", f"Simulating creation of custom instance configuration '{effective_config}' with {len(clean_opt_replicas)} optional read replica(s)...")
                    await asyncio.sleep(0.8)
                    self._log(campaign, "SUCCESS", "spanner", f"Simulated custom instance config '{effective_config}' ready.")
                self._log(campaign, "INFO", "spanner", f"Simulating provisioning 1-node instance '{instance_id}' on {effective_config}...")
                await asyncio.sleep(1.2)
                self._log(campaign, "SUCCESS", "spanner", f"Simulated instance '{instance_id}' provisioned.")

            if step2:
                _finish_step(step2, StepStatus.COMPLETED)
            self._save_to_disk()

            # -------------------------------------------------------------
            # Step 3: Database & Schema Initialization
            # -------------------------------------------------------------
            step3 = next((s for s in campaign.steps if s.id == "create_database"), None)
            if step3:
                step3.status = StepStatus.RUNNING
                step3.started_at = datetime.now(timezone.utc).isoformat()
            db_id = campaign.spanner_database_id or "benchmark-db"
            campaign.spanner_database_id = db_id
            campaign.progress_pct = 35
            campaign.message = f"Initializing database '{db_id}' and schema table 'LatencyKV'..."
            self._log(campaign, "INFO", "database", f"Creating database '{db_id}' with schema: CREATE TABLE LatencyKV (id STRING(64) NOT NULL, created_at TIMESTAMP OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (id)...")
            self._save_to_disk()

            if campaign.execution_mode == "live" and os.path.exists(gcloud_bin):
                ddl = "CREATE TABLE LatencyKV (id STRING(64) NOT NULL, created_at TIMESTAMP OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (id)"
                db_cmd = [
                    gcloud_bin, "spanner", "databases", "create", db_id,
                    f"--instance={instance_id}",
                    f"--project={campaign.project_id}",
                    f"--ddl={ddl}",
                ]
                db_proc = await asyncio.create_subprocess_exec(
                    *db_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
                )
                db_out, _ = await db_proc.communicate()
                db_text = db_out.decode("utf-8", errors="replace").strip()
                if db_text:
                    self._log(campaign, "INFO", "database", db_text)
                if db_proc.returncode != 0 and "ALREADY_EXISTS" not in db_text:
                    raise RuntimeError(f"Failed to create database '{db_id}': {db_text}")
                self._log(campaign, "SUCCESS", "database", f"Database '{db_id}' with table 'LatencyKV' created successfully.")

                # If a custom leader region is designated for multi-region instance, update default_leader on database
                rows = self.spanner_service.loader.get_config(campaign.spanner_config) or []
                default_leader_row = next((r for r in rows if r["type"] == "leader"), None)
                default_leader = default_leader_row["region"] if default_leader_row else ""
                has_custom_leader = bool(campaign.leader_region and default_leader_row and campaign.leader_region != default_leader)

                if has_custom_leader:
                    self._log(campaign, "INFO", "database", f"Applying custom default leader '{campaign.leader_region}' (default is '{default_leader}') to database '{db_id}'...")
                    alter_ddl = f"ALTER DATABASE `{db_id}` SET OPTIONS (default_leader = '{campaign.leader_region}')"
                    alter_cmd = [
                        gcloud_bin, "spanner", "databases", "ddl", "update", db_id,
                        f"--instance={instance_id}",
                        f"--project={campaign.project_id}",
                        f"--ddl={alter_ddl}",
                    ]
                    alter_proc = await asyncio.create_subprocess_exec(
                        *alter_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
                    )
                    alter_out, _ = await alter_proc.communicate()
                    alter_text = alter_out.decode("utf-8", errors="replace").strip()
                    if alter_text:
                        self._log(campaign, "INFO", "database", alter_text)
                    if alter_proc.returncode != 0:
                        raise RuntimeError(f"Failed to configure custom leader '{campaign.leader_region}' on database '{db_id}': {alter_text}")
                    self._log(campaign, "SUCCESS", "database", f"Custom default leader '{campaign.leader_region}' successfully applied to database '{db_id}'.")
            else:
                self._log(campaign, "INFO", "database", f"Simulating database '{db_id}' creation and table 'LatencyKV' DDL...")
                await asyncio.sleep(1.2)
                rows = self.spanner_service.loader.get_config(campaign.spanner_config) or []
                default_leader_row = next((r for r in rows if r["type"] == "leader"), None)
                default_leader = default_leader_row["region"] if default_leader_row else ""
                if campaign.leader_region and default_leader_row and campaign.leader_region != default_leader:
                    self._log(campaign, "INFO", "database", f"[Dry Run] Simulating custom default leader '{campaign.leader_region}' (default is '{default_leader}') on database '{db_id}'...")
                self._log(campaign, "SUCCESS", "database", f"Database '{db_id}' and table 'LatencyKV' ready.")

            if step3:
                _finish_step(step3, StepStatus.COMPLETED)
            self._save_to_disk()

            # -------------------------------------------------------------
            # Step 4: Client Runner Setup
            # -------------------------------------------------------------
            step4 = next((s for s in campaign.steps if s.id == "setup_clients"), None)
            if step4:
                step4.status = StepStatus.RUNNING
                step4.started_at = datetime.now(timezone.utc).isoformat()
            campaign.status = CampaignState.PROVISIONING_VMS
            campaign.progress_pct = 40
            campaign.message = f"Setting up GCE runner VMs across {len(campaign.client_regions)} client regions..."
            self._log(campaign, "INFO", "clients", f"Setting up GCE client runners across {len(campaign.client_regions)} regions: {', '.join(campaign.client_regions)}...")
            for r in campaign.region_results.values():
                r.status = "provisioning"
                r.progress_pct = 25
            self._save_to_disk()

            startup_script = BACKEND_DIR / "app" / "templates" / "runner_startup_script.sh"
            if not startup_script.exists():
                startup_script = BACKEND_DIR / "templates" / "runner_startup_script.sh"
            if not startup_script.exists():
                raise FileNotFoundError(f"Runner startup script not found at {startup_script}")

            if campaign.execution_mode == "live" and os.path.exists(gcloud_bin):
                jar_gcs_path = await self._ensure_staging_jar(campaign.project_id, gcloud_bin, campaign)
                if not jar_gcs_path:
                    detail = campaign.message or "Runner JAR missing or failed to upload to Cloud Storage bucket"
                    raise RuntimeError(f"Failed to stage benchmark runner JAR to Cloud Storage: {detail}")

                # Provision VMs concurrently across client regions
                prov_tasks = [
                    self._provision_client_vm(
                        region, campaign, gcloud_bin, jar_gcs_path, instance_id, db_id, startup_script
                    )
                    for region in campaign.client_regions
                ]
                prov_results = await asyncio.gather(*prov_tasks)
                has_prov_failure = False
                for region, vm_entry, success, err in prov_results:
                    if success:
                        campaign.gce_instances[region] = vm_entry
                    else:
                        has_prov_failure = True
                        campaign.region_results[region].status = "failed"
                        campaign.region_results[region].error_message = err

                if has_prov_failure and not campaign.gce_instances:
                    raise RuntimeError("Failed to provision GCE runner VMs in any requested region.")
            else:
                self._log(campaign, "INFO", "clients", f"[Dry Run] Simulating client runner provisioning across {len(campaign.client_regions)} regions...")
                await asyncio.sleep(1.2)
                self._log(campaign, "SUCCESS", "clients", f"[Dry Run] Simulated runner provisioning complete.")

            if step4:
                _finish_step(step4, StepStatus.COMPLETED)
            self._save_to_disk()

            # -------------------------------------------------------------
            # Step 5: Latency Workload Execution
            # -------------------------------------------------------------
            step5 = next((s for s in campaign.steps if s.id == "execute_workload"), None)
            if step5:
                step5.status = StepStatus.RUNNING
                step5.started_at = datetime.now(timezone.utc).isoformat()
            campaign.status = CampaignState.RUNNING
            campaign.progress_pct = 60
            campaign.message = f"Executing {campaign.operations} operations via DirectPath across {len(campaign.client_regions)} regions..."
            self._log(campaign, "INFO", "workload", f"Executing workload operations ({campaign.operations} ops per workload, 15s staleness bound)...")
            for r in campaign.region_results.values():
                if r.status != "failed":
                    r.status = "running"
                    r.progress_pct = 50
            self._save_to_disk()

            workload_failed = False
            workload_error = None
            raw_vm_results: Dict[str, dict] = {}

            if campaign.execution_mode == "live" and campaign.gce_instances and os.path.exists(gcloud_bin):
                self._log(campaign, "INFO", "workload", f"Awaiting workload telemetry from {len(campaign.gce_instances)} active GCE VMs...")
                poll_tasks = [
                    self._poll_vm_results(region, vm_entry, campaign, gcloud_bin)
                    for region, vm_entry in campaign.gce_instances.items()
                ]
                poll_results = await asyncio.gather(*poll_tasks)
                for region, data, err in poll_results:
                    if err or not data:
                        self._log(campaign, "ERROR", "workload", f"Workload on {region} failed: {err}")
                        campaign.region_results[region].status = "failed"
                        campaign.region_results[region].error_message = err
                        workload_failed = True
                        workload_error = err
                    else:
                        raw_vm_results[region] = data
                        r = campaign.region_results[region]
                        r.status = "completed"
                        r.progress_pct = 100
                        r.direct_path_confirmed = bool(data.get("directPathConfirmed", False))

                        def _parse_pct(d: Any) -> LatencyMetrics:
                            if not d or not isinstance(d, dict):
                                return LatencyMetrics()
                            return LatencyMetrics(
                                ops_count=int(d.get("opsCount", campaign.operations)),
                                hit_rate=float(d.get("hitRate", 1.0)),
                                p50=float(d.get("p50", 0.0)),
                                p90=float(d.get("p90", 0.0)),
                                p95=float(d.get("p95", 0.0)),
                                p99=float(d.get("p99", 0.0)),
                                min=float(d.get("min", 0.0)),
                                max=float(d.get("max", 0.0)),
                                avg=float(d.get("avg", 0.0)),
                            )

                        r.writes = _parse_pct(data.get("writes"))
                        r.strong_reads = _parse_pct(data.get("strongReads"))
                        r.stale_reads = _parse_pct(data.get("staleReads"))
                        direct_label = "DirectPath ALTS" if r.direct_path_confirmed else "GFE"
                        self._log(
                            campaign, "SUCCESS", "workload",
                            f"Region {region}: Write p50={r.writes.p50}ms, Strong Read p50={r.strong_reads.p50}ms, Stale Read p50={r.stale_reads.p50}ms ({direct_label})."
                        )
            else:
                self._log(campaign, "INFO", "workload", f"[Dry Run] Simulating distributed workload execution...")
                await asyncio.sleep(1.5)
                self._log(campaign, "SUCCESS", "workload", f"[Dry Run] Workload simulation complete.")

            if workload_failed and not raw_vm_results:
                self._log(campaign, "ERROR", "workload", f"Workload execution failed across all regions: {workload_error}")
                if step5:
                    _finish_step(step5, StepStatus.FAILED, error_message=workload_error)
                if step6_ref := next((s for s in campaign.steps if s.id == "aggregate_metrics"), None):
                    _finish_step(step6_ref, StepStatus.SKIPPED)
                self._save_to_disk()
            else:
                self._log(campaign, "SUCCESS", "workload", "Completed workload operations across all reporting client regions.")
                if step5:
                    _finish_step(step5, StepStatus.COMPLETED)
                self._save_to_disk()

            # -------------------------------------------------------------
            # Step 6: Telemetry & Results Analysis
            # -------------------------------------------------------------
            step6 = next((s for s in campaign.steps if s.id == "aggregate_metrics"), None)
            if not (workload_failed and not raw_vm_results):
                if step6:
                    step6.status = StepStatus.RUNNING
                    step6.started_at = datetime.now(timezone.utc).isoformat()
                campaign.progress_pct = 85
                campaign.message = "Aggregating latency telemetry and computing percentile distributions..."
                self._log(campaign, "INFO", "telemetry", "Computing percentile distributions (p50, p90, p95, p99, min, max, avg)...")

                leader_coords = (41.2619, -95.8608)
                if config_detail:
                    lc = next(((r.lat, r.lon) for r in config_detail.replicas if r.is_leader), None)
                    if lc:
                        leader_coords = lc

                for region_id, r in campaign.region_results.items():
                    meta = CLIENT_REGIONS_CATALOG.get(region_id, (region_id, "Unknown", 41.2619, -95.8608))
                    client_lat, client_lon = meta[2], meta[3]
                    dist_to_leader = haversine_distance_km(client_lat, client_lon, leader_coords[0], leader_coords[1])
                    if region_id == campaign.leader_region:
                        dist_to_leader = 0.0
                    r.distance_to_leader_km = round(dist_to_leader, 1)

                    has_local_replica = False
                    opt_set = set(campaign.optional_replicas or [])
                    if config_detail:
                        # Provisioned replicas include base quorum replicas and provisioned optional replicas
                        provisioned_replicas = [
                            rep for rep in config_detail.replicas
                            if rep.replica_type != ReplicaType.OPTIONAL_READ_ONLY or rep.region in opt_set
                        ]
                        has_local_replica = any(
                            (not rep.is_witness)
                            and haversine_distance_km(client_lat, client_lon, rep.lat, rep.lon) < 250
                            for rep in provisioned_replicas
                        )
                        if not r.replica_type:
                            for rep in config_detail.replicas:
                                if rep.region == region_id:
                                    if (
                                        rep.is_leader
                                        or rep.replica_type == ReplicaType.LEADER
                                        or region_id == campaign.leader_region
                                    ):
                                        r.replica_type = "Leader Region"
                                    elif rep.replica_type == ReplicaType.RW_REPLICA:
                                        r.replica_type = "R/W Replica Region"
                                    elif rep.replica_type == ReplicaType.WITNESS or rep.is_witness:
                                        r.replica_type = "Witness Region"
                                    elif rep.replica_type == ReplicaType.READ_ONLY or rep.is_read_only:
                                        r.replica_type = "Read Only Region"
                                    elif rep.replica_type == ReplicaType.OPTIONAL_READ_ONLY:
                                        if rep.region in opt_set:
                                            r.replica_type = "Optional Read Only (Provisioned)"
                                        else:
                                            r.replica_type = "Optional Read Only (Unprovisioned)"
                                    break
                    r.has_local_replica = has_local_replica

                    # If region was measured by live GCE VM, keep its real metrics
                    if region_id in raw_vm_results:
                        r.status = "completed"
                        r.progress_pct = 100
                    elif campaign.execution_mode == "dry_run":
                        # Dry run mode: synthesize modeled estimates
                        fiber_rtt_to_leader = max(1.2, dist_to_leader * 0.0105)
                        direct_access_deduction = 2.4
                        paxos_quorum_delay = 3.5
                        if config_detail and len(config_detail.replicas) > 1:
                            base_reps_for_voting = [
                                rep for rep in config_detail.replicas
                                if rep.replica_type != ReplicaType.OPTIONAL_READ_ONLY
                            ]
                            voting_dists = [
                                haversine_distance_km(leader_coords[0], leader_coords[1], rep.lat, rep.lon)
                                for rep in base_reps_for_voting
                                if not rep.is_leader and not rep.is_witness
                            ]
                            if voting_dists:
                                paxos_quorum_delay = max(3.0, min(voting_dists) * 0.009)

                        base_write = max(
                            5.5,
                            (fiber_rtt_to_leader - direct_access_deduction) + paxos_quorum_delay + random.uniform(1.8, 3.2),
                        )
                        r.writes = _generate_percentile_metrics(base_write, campaign.operations)

                        base_strong_read = max(
                            1.6,
                            (fiber_rtt_to_leader - direct_access_deduction) + random.uniform(0.8, 1.8),
                        )
                        r.strong_reads = _generate_percentile_metrics(base_strong_read, campaign.operations)

                        if has_local_replica:
                            base_stale_read = random.uniform(1.6, 2.8)
                        else:
                            nearest_replica_dist = dist_to_leader
                            if config_detail:
                                base_serving_reps = [
                                    rep for rep in config_detail.replicas
                                    if (rep.replica_type != ReplicaType.OPTIONAL_READ_ONLY or rep.region in opt_set)
                                    and not rep.is_witness
                                ]
                                replica_dists = [
                                    haversine_distance_km(client_lat, client_lon, rep.lat, rep.lon)
                                    for rep in base_serving_reps
                                ]
                                if replica_dists:
                                    nearest_replica_dist = min(replica_dists)
                            fiber_rtt_nearest = max(1.2, nearest_replica_dist * 0.0105)
                            base_stale_read = max(
                                1.8,
                                (fiber_rtt_nearest - direct_access_deduction) + random.uniform(0.8, 1.8),
                            )
                        r.stale_reads = _generate_percentile_metrics(base_stale_read, campaign.operations)
                        r.status = "completed"
                        r.progress_pct = 100

                    self._log(campaign, "SUCCESS", "telemetry", f"Region {region_id}: Write p50={r.writes.p50}ms, Strong Read p50={r.strong_reads.p50}ms, Stale Read p50={r.stale_reads.p50}ms.")

                if step6:
                    _finish_step(step6, StepStatus.COMPLETED)
                self._save_to_disk()

            # -------------------------------------------------------------
            # Step 7: Resource Teardown
            # -------------------------------------------------------------
            step7 = next((s for s in campaign.steps if s.id == "teardown"), None)
            if campaign.auto_teardown and campaign.execution_mode == "live" and os.path.exists(gcloud_bin):
                if step7:
                    step7.status = StepStatus.RUNNING
                    step7.started_at = datetime.now(timezone.utc).isoformat()
                campaign.progress_pct = 92
                campaign.message = "Auto-teardown: Deleting ephemeral GCE runner VMs and Cloud Spanner instance..."
                self._log(campaign, "INFO", "teardown", "Auto-teardown enabled: deleting GCE client VMs and Spanner instance to prevent ongoing cloud billing...")
                self._save_to_disk()

                # Delete GCE VMs in parallel
                if campaign.gce_instances:
                    del_vm_tasks = []
                    for reg, entry in list(campaign.gce_instances.items()):
                        vm_name, zone = entry.split(":")
                        del_vm_tasks.append(
                            asyncio.create_subprocess_exec(
                                gcloud_bin, "compute", "instances", "delete", vm_name,
                                f"--zone={zone}", f"--project={campaign.project_id}", "--quiet",
                                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                            )
                        )
                    procs = await asyncio.gather(*del_vm_tasks)
                    for p in procs:
                        await p.communicate()
                    self._log(campaign, "SUCCESS", "teardown", f"Deleted {len(campaign.gce_instances)} ephemeral client VMs across tested regions.")
                    campaign.gce_instances = {}

                # Delete Spanner instance
                if instance_id:
                    del_proc = await asyncio.create_subprocess_exec(
                        gcloud_bin, "spanner", "instances", "delete", instance_id,
                        f"--project={campaign.project_id}", "--quiet",
                        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
                    )
                    await del_proc.communicate()
                    campaign.spanner_instance_id = None
                    campaign.spanner_database_id = None
                    self._log(campaign, "SUCCESS", "teardown", f"Deleted Spanner instance '{instance_id}'.")

                # Delete custom Spanner instance config if created
                clean_opt_replicas = [r for r in (campaign.optional_replicas or []) if r]
                if clean_opt_replicas:
                    custom_config_id = f"custom-{campaign.spanner_config}-{campaign.campaign_id[:8]}"
                    cfg_del_proc = await asyncio.create_subprocess_exec(
                        gcloud_bin, "spanner", "instance-configs", "delete", custom_config_id,
                        f"--project={campaign.project_id}", "--quiet",
                        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
                    )
                    await cfg_del_proc.communicate()
                    self._log(campaign, "SUCCESS", "teardown", f"Deleted custom Spanner instance config '{custom_config_id}'.")

                if step7:
                    _finish_step(step7, StepStatus.COMPLETED)
            else:
                if step7:
                    _finish_step(step7, StepStatus.SKIPPED)
                self._log(campaign, "INFO", "teardown", "Cloud resources retained (auto-teardown disabled or dry-run). Use 'Tear Down Resources' to clean up at any time.")


            # Final State Transition (AFTER Step 7 completes)
            if workload_failed:
                campaign.status = CampaignState.FAILED
                campaign.message = f"Benchmark campaign failed: {workload_error}"
            else:
                campaign.status = CampaignState.COMPLETED
                campaign.message = f"Benchmark campaign completed successfully across {len(campaign.client_regions)} regions."
            campaign.progress_pct = 100
            campaign.completed_at = datetime.now(timezone.utc).isoformat()
            self._save_to_disk()

        except asyncio.CancelledError:
            self._log(campaign, "WARN", "lifecycle", "Campaign task was cancelled.")
            if campaign:
                self._emergency_cleanup_resources(campaign, gcloud_bin, instance_id)
                self._save_to_disk()
        except Exception as e:
            logger.exception("Campaign orchestrator failed")
            campaign = self._campaigns.get(campaign_id)
            if campaign:
                campaign.status = CampaignState.FAILED
                campaign.message = f"Campaign failed: {str(e)}"
                campaign.completed_at = datetime.now(timezone.utc).isoformat()
                self._log(campaign, "ERROR", "error", f"Unhandled exception in orchestrator: {str(e)}")
                if campaign.steps:
                    for s in campaign.steps:
                        if s.status == StepStatus.RUNNING:
                            _finish_step(s, StepStatus.FAILED, error_message=str(e))
                self._emergency_cleanup_resources(campaign, gcloud_bin, instance_id)
                self._save_to_disk()


_benchmark_service_instance: Optional[BenchmarkService] = None


def get_benchmark_service() -> BenchmarkService:
    """Returns singleton instance of BenchmarkService."""
    global _benchmark_service_instance
    if _benchmark_service_instance is None:
        _benchmark_service_instance = BenchmarkService()
    return _benchmark_service_instance
