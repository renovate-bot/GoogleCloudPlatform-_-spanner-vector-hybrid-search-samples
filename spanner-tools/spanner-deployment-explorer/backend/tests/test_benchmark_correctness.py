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

import math
from pathlib import Path
import pytest
from app.models.benchmark import LatencyMetrics, RegionBenchmarkResult
from app.services.benchmark_service import BenchmarkService


def test_witness_excluded_from_local_replica():
    """
    eur3 configuration has:
      - europe-west1 (default leader)
      - europe-west4 (r/w replica)
      - europe-north1 (witness)
    A client in europe-north1 must NOT be marked as having a local replica
    because witnesses cannot serve reads! Also validates replica_type role assignment.
    """
    service = BenchmarkService()
    leader_region, results = service._compute_region_results(
        spanner_config="eur3",
        client_regions=["europe-north1", "europe-west1", "europe-west4", "us-central1", "asia-east1"],
    )
    assert "europe-north1" in results
    assert "europe-west1" in results
    assert "europe-west4" in results
    assert "us-central1" in results
    assert "asia-east1" in results
    # europe-north1 is a witness: has_local_replica is False, role is Witness Region
    assert results["europe-north1"].has_local_replica is False
    assert results["europe-north1"].replica_type == "Witness Region"
    # europe-west1 is leader: has_local_replica is True, role is Leader Region
    assert results["europe-west1"].has_local_replica is True
    assert results["europe-west1"].replica_type == "Leader Region"
    # europe-west4 is read-write: has_local_replica is True, role is R/W Replica Region
    assert results["europe-west4"].has_local_replica is True
    assert results["europe-west4"].replica_type == "R/W Replica Region"
    # us-central1 is an optional read replica (unprovisioned in standard benchmark): has_local_replica is False
    assert results["us-central1"].has_local_replica is False
    assert results["us-central1"].replica_type == "Optional Read Only (Unprovisioned)"
    # asia-east1 has no replica in eur3: has_local_replica is False and replica_type is None
    assert results["asia-east1"].has_local_replica is False
    assert results["asia-east1"].replica_type is None


def test_replica_type_custom_leader_and_read_only():
    """
    Validates replica_type assignment when custom leader is selected,
    and for configs with read-only replicas (eur6).
    """
    service = BenchmarkService()
    # 1. Custom leader on eur3 switched to europe-west4
    leader_region, results = service._compute_region_results(
        spanner_config="eur3",
        client_regions=["europe-west1", "europe-west4", "europe-north1"],
        custom_leader="europe-west4",
    )
    assert leader_region == "europe-west4"
    assert results["europe-west4"].replica_type == "Leader Region"
    assert results["europe-west1"].replica_type == "R/W Replica Region"
    assert results["europe-north1"].replica_type == "Witness Region"

    # 2. Config with mandatory Read Only replica (nam6 has us-west1 as mandatory read-only)
    leader_region, results = service._compute_region_results(
        spanner_config="nam6",
        client_regions=["us-west1"],
    )
    assert results["us-west1"].replica_type == "Read Only Region"
    assert results["us-west1"].has_local_replica is True

    # 3. Config with Optional Read Only replica (eur6 has us-east1 as optional read-only)
    leader_region, results_eur6 = service._compute_region_results(
        spanner_config="eur6",
        client_regions=["us-east1"],
    )
    assert results_eur6["us-east1"].replica_type == "Optional Read Only (Unprovisioned)"
    assert results_eur6["us-east1"].has_local_replica is False


def test_nearest_rank_percentile_math():
    """
    Tests nearest-rank indexing: idx = ceil(p * N) - 1.
    For N=100 elements (1.0 to 100.0):
      p50 -> ceil(0.50 * 100) - 1 = 50 - 1 = 49 -> value 50.0
      p90 -> ceil(0.90 * 100) - 1 = 90 - 1 = 89 -> value 90.0
      p95 -> ceil(0.95 * 100) - 1 = 95 - 1 = 94 -> value 95.0
      p99 -> ceil(0.99 * 100) - 1 = 99 - 1 = 98 -> value 99.0
    """
    values = [float(i) for i in range(1, 101)]
    def percentile(sorted_list, p):
        if not sorted_list:
            return 0.0
        idx = int(math.ceil(p * len(sorted_list))) - 1
        idx = max(0, min(len(sorted_list) - 1, idx))
        return sorted_list[idx]

    assert percentile(values, 0.50) == 50.0
    assert percentile(values, 0.90) == 90.0
    assert percentile(values, 0.95) == 95.0
    assert percentile(values, 0.99) == 99.0


def test_latency_metrics_hit_rate_and_direct_path():
    """Validates hit_rate on LatencyMetrics and direct_path_confirmed on RegionBenchmarkResult."""
    metrics = LatencyMetrics(ops_count=100, hit_rate=0.98, p50=2.3)
    assert metrics.hit_rate == 0.98
    assert metrics.ops_count == 100

    empty_metrics = LatencyMetrics()
    assert empty_metrics.hit_rate == 1.0
    assert empty_metrics.ops_count == 0

    res = RegionBenchmarkResult(
        region="us-central1",
        region_name="Iowa",
        direct_path_confirmed=False,
    )
    assert res.direct_path_confirmed is False
    assert res.has_local_replica is False


def test_spanner_instance_config_prefix_resolution():
    """
    Validates canonical GCP instanceConfig identifier mapping:
    - Regional configs in Spanner REQUIRE the 'regional-' prefix in GCP (e.g. regional-europe-central2).
    - Multi-region configs do not take 'regional-' (e.g. eur3).
    - Dual-region configs already have 'dual-region-' (e.g. dual-region-germany1).
    - Already prefixed 'regional-' inputs are preserved.
    """
    from app.services.data_loader import DataLoader
    from app.services.spanner_service import SpannerService

    loader = DataLoader.get_instance()
    service = SpannerService(data_loader=loader)

    # Regional configs
    assert loader.get_gcp_instance_config("europe-central2") == "regional-europe-central2"
    assert loader.get_gcp_instance_config("us-central1") == "regional-us-central1"
    assert loader.get_gcp_instance_config("regional-europe-central2") == "regional-europe-central2"

    # Multi-region configs
    assert loader.get_gcp_instance_config("eur3") == "eur3"
    assert loader.get_gcp_instance_config("nam-eur-asia1") == "nam-eur-asia1"

    # Dual-region configs
    assert loader.get_gcp_instance_config("dual-region-germany1") == "dual-region-germany1"

    # Service delegation
    assert service.get_gcp_instance_config("europe-central2") == "regional-europe-central2"
    assert service.get_gcp_instance_config("eur3") == "eur3"

    # DataLoader.get_config handles both forms
    config_by_plain = loader.get_config("europe-central2")
    config_by_prefixed = loader.get_config("regional-europe-central2")
    assert config_by_plain is not None
    assert config_by_prefixed is not None
    assert config_by_plain == config_by_prefixed


def test_gce_runner_startup_script_template():
    """Validates the presence and critical markers in the GCE runner startup script."""
    template_path = Path(__file__).resolve().parent.parent / "app" / "templates" / "runner_startup_script.sh"
    assert template_path.exists(), f"Startup script template missing at {template_path}"
    content = template_path.read_text(encoding="utf-8")
    assert "Metadata-Flavor: Google" in content
    assert "guest-attributes/dbexplorer/results" in content
    assert "DBEXPLORER_BENCHMARK_OUTPUT_BEGIN" in content
    assert "DBEXPLORER_BENCHMARK_OUTPUT_END" in content
    assert "java -jar" in content
    assert "jar_gcs_path" in content


def test_gce_instances_model_field():
    """Validates gce_instances field on BenchmarkCampaignStatus."""
    from app.models.benchmark import BenchmarkCampaignStatus, CampaignState
    status = BenchmarkCampaignStatus(
        campaign_id="test-cid-123",
        project_id="test-project",
        spanner_config="eur3",
        status=CampaignState.RUNNING,
        execution_mode="live",
        created_at="2026-09-11T10:00:00Z",
        gce_instances={"europe-west1": "dbbench-test-europe-west1:europe-west1-b"},
    )
    assert "europe-west1" in status.gce_instances
    assert status.gce_instances["europe-west1"] == "dbbench-test-europe-west1:europe-west1-b"
    data = status.model_dump()
    assert data["gce_instances"]["europe-west1"] == "dbbench-test-europe-west1:europe-west1-b"


@pytest.mark.asyncio
async def test_zone_resolution_fallback():
    """Validates _resolve_zone_for_region fallback."""
    service = BenchmarkService()
    zone = await service._resolve_zone_for_region("europe-west1", "test-project", "/nonexistent/gcloud")
    assert zone.startswith("europe-west1-")


def test_staging_bucket_models():
    """Validates staging_bucket field across benchmark and preflight models."""
    from app.models.benchmark import BenchmarkCampaignConfig, BenchmarkCampaignStatus, BenchmarkCampaignUpdate, CampaignState
    from app.models.preflight import PreflightCheckRequest

    cfg = BenchmarkCampaignConfig(
        project_id="test-project",
        spanner_config="eur3",
        client_regions=["europe-west1"],
        staging_bucket="gs://my-custom-bucket",
    )
    assert cfg.staging_bucket == "gs://my-custom-bucket"

    update = BenchmarkCampaignUpdate(staging_bucket="new-bucket")
    assert update.staging_bucket == "new-bucket"

    status = BenchmarkCampaignStatus(
        campaign_id="test-cmp",
        project_id="test-project",
        spanner_config="eur3",
        status=CampaignState.DRAFT,
        execution_mode="live",
        created_at="2026-09-11T10:00:00Z",
        staging_bucket="my-custom-bucket",
    )
    assert status.staging_bucket == "my-custom-bucket"

    preflight_req = PreflightCheckRequest(
        project_id="test-project",
        client_regions=["europe-west1"],
        spanner_config="eur3",
        staging_bucket="gs://my-custom-bucket",
    )
    assert preflight_req.staging_bucket == "gs://my-custom-bucket"


@pytest.mark.asyncio
async def test_preflight_dry_run_includes_storage_staging():
    """Verifies that dry-run preflight checks include storage staging verification."""
    from app.services.preflight_service import PreflightService
    from app.models.preflight import PreflightCheckRequest, PreflightCheckStatus

    service = PreflightService()
    req = PreflightCheckRequest(
        project_id="test-project",
        client_regions=["europe-west1"],
        spanner_config="eur3",
        execution_mode="dry_run",
        staging_bucket="gs://custom-staging-bucket",
    )
    res = await service.execute_preflight(req)
    assert res.all_passed is True

    storage_check = next((c for c in res.checks if c.id == "storage_staging"), None)
    assert storage_check is not None
    assert storage_check.status == PreflightCheckStatus.PASSED
    assert "custom-staging-bucket" in storage_check.message


def test_runner_startup_script_path_resolution():
    """Validates that the orchestrator's startup_script path actually exists on disk."""
    from app.services.benchmark_service import BACKEND_DIR
    startup_script = BACKEND_DIR / "app" / "templates" / "runner_startup_script.sh"
    if not startup_script.exists():
        startup_script = BACKEND_DIR / "templates" / "runner_startup_script.sh"
    assert startup_script.exists(), f"Startup script not found at {startup_script}"
    assert startup_script.is_file()
    assert startup_script.stat().st_size > 500


def test_emergency_cleanup_resources():
    """Validates that _emergency_cleanup_resources clears state without raising errors."""
    from app.models.benchmark import BenchmarkCampaignStatus, CampaignState
    service = BenchmarkService()
    status = BenchmarkCampaignStatus(
        campaign_id="cmp-emergency-test",
        project_id="test-project",
        spanner_config="eur3",
        status=CampaignState.RUNNING,
        execution_mode="live",
        auto_teardown=True,
        spanner_instance_id="spanner-mock-id",
        created_at="2026-09-11T10:00:00Z",
        gce_instances={"europe-west1": "mock-vm:europe-west1-b"},
    )
    # Call emergency cleanup with mock gcloud path - should not raise and should clear local dictionaries
    service._emergency_cleanup_resources(status, "/nonexistent/gcloud", "spanner-mock-id")
    assert status.gce_instances == {}


def test_find_mvn_binary_and_runner_jar():
    """Validates that find_mvn_binary finds a Maven executable and the benchmark JAR exists."""
    import os
    from app.services.benchmark_service import BACKEND_DIR, find_mvn_binary

    mvn = find_mvn_binary()
    assert mvn is not None, "Expected find_mvn_binary() to locate maven in environment or Homebrew paths"
    assert os.path.exists(mvn)
    assert os.access(mvn, os.X_OK)

    jar_path = BACKEND_DIR.parent / "benchmark" / "target" / "spanner-direct-access-benchmark-1.0.0.jar"
    assert jar_path.exists(), f"Expected runner JAR to exist at {jar_path}"
    assert jar_path.stat().st_size > 10_000_000, f"Expected shaded uber-jar > 10MB, got {jar_path.stat().st_size}"


@pytest.mark.asyncio
async def test_ensure_staging_jar_diagnostics():
    """Validates that _ensure_staging_jar populates campaign.message when staging cannot proceed."""
    from app.models.benchmark import BenchmarkCampaignStatus, CampaignState
    from pathlib import Path
    from unittest.mock import patch

    service = BenchmarkService()
    campaign = BenchmarkCampaignStatus(
        campaign_id="cmp-staging-test",
        project_id="test-project",
        spanner_config="eur3",
        status=CampaignState.RUNNING,
        execution_mode="live",
        created_at="2026-09-11T10:00:00Z",
    )

    with patch("app.services.benchmark_service.find_mvn_binary", return_value=None), \
         patch.object(Path, "exists", return_value=False):
        res = await service._ensure_staging_jar("test-project", "/nonexistent/gcloud", campaign)
        assert res is None
        assert campaign.message is not None
        assert "spanner-direct-access-benchmark-1.0.0.jar" in campaign.message


def test_benchmark_campaign_with_optional_read_replicas():
    """Validates that campaign creation and region calculation distinguishes provisioned vs unprovisioned optional replicas."""
    from app.models.benchmark import BenchmarkCampaignConfig
    from app.services.benchmark_service import BenchmarkService

    service = BenchmarkService()
    # nam3 has optional read replicas in us-west1 and europe-west1
    config = BenchmarkCampaignConfig(
        project_id="test-project",
        spanner_config="nam3",
        client_regions=["us-west1", "europe-west1"],
        optional_replicas=["us-west1"],
    )
    campaign = service.create_campaign(config)
    assert campaign.optional_replicas == ["us-west1"]

    # us-west1 is in optional_replicas -> Provisioned & has_local_replica True
    res_us = campaign.region_results["us-west1"]
    assert res_us.replica_type == "Optional Read Only (Provisioned)"
    assert res_us.has_local_replica is True

    # europe-west1 is not in optional_replicas -> Unprovisioned & has_local_replica False
    res_eu = campaign.region_results["europe-west1"]
    assert res_eu.replica_type == "Optional Read Only (Unprovisioned)"
    assert res_eu.has_local_replica is False



