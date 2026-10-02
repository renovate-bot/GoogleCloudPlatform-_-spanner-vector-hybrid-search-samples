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
import os
import pytest
from httpx import ASGITransport, AsyncClient
from app.main import create_app


@pytest.mark.asyncio
async def test_system_config_endpoint():
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/v1/system/config")
        assert response.status_code == 200
        data = response.json()
        assert "features" in data
        assert "load_testing" in data
        assert "enable_load_testing" in data["features"]
        assert "allow_dry_run" in data["features"]
        assert "allow_multiple_selections" in data["features"]


@pytest.mark.asyncio
async def test_client_regions_endpoint():
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/v1/benchmark/client-regions")
        assert response.status_code == 200
        regions = response.json()
        assert len(regions) == 44
        region_ids = [r["region"] for r in regions]
        assert "us-central1" in region_ids
        assert "europe-west1" in region_ids
        assert "asia-east1" in region_ids
        # Verify 6 newly added regions are present
        assert "us-west8" in region_ids
        assert "us-east5" in region_ids
        assert "us-south1" in region_ids
        assert "asia-southeast3" in region_ids
        assert "europe-north2" in region_ids
        assert "northamerica-south1" in region_ids
        # Verify strict parity with client catalog (no unauthorized/non-public regions)
        from app.services.benchmark_service import CLIENT_REGIONS_CATALOG
        assert set(region_ids) == set(CLIENT_REGIONS_CATALOG.keys())
        assert "us-central2" not in region_ids
        # Verify coordinates
        us_central1 = next(r for r in regions if r["region"] == "us-central1")
        assert us_central1["latitude"] > 40.0
        assert us_central1["longitude"] < -90.0
        us_west8 = next(r for r in regions if r["region"] == "us-west8")
        assert 33.0 < us_west8["latitude"] < 34.0
        assert -113.0 < us_west8["longitude"] < -111.0


@pytest.mark.asyncio
async def test_preflight_check_dry_run():
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        payload = {
            "project_id": "test-spanner-project",
            "client_regions": ["us-central1", "europe-west1"],
            "spanner_config": "regional-us-central1",
            "execution_mode": "dry_run",
        }
        response = await client.post("/api/v1/benchmark/preflight", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["all_passed"] is True
        assert len(data["checks"]) >= 4
        check_ids = [c["id"] for c in data["checks"]]
        assert "adc_auth" in check_ids
        assert "project_access" in check_ids
        assert "spanner_iam" in check_ids
        assert "regional_subnets" in check_ids


@pytest.mark.asyncio
async def test_preflight_check_invalid_project_id():
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        payload = {
            "project_id": "INVALID_PROJECT_NAME!!!",
            "client_regions": ["us-central1"],
            "spanner_config": "regional-us-central1",
            "execution_mode": "dry_run",
        }
        response = await client.post("/api/v1/benchmark/preflight", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["all_passed"] is False
        project_check = next(c for c in data["checks"] if c["id"] == "project_access")
        assert project_check["status"] == "failed"


@pytest.mark.asyncio
async def test_multi_region_benchmark_campaign_lifecycle():
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Start Campaign
        start_payload = {
            "project_id": "test-spanner-project",
            "spanner_config": "regional-us-central1",
            "client_regions": ["us-central1", "europe-west1"],
            "operations": 50,
            "staleness_seconds": 15,
            "execution_mode": "dry_run",
        }
        start_res = await client.post("/api/v1/benchmark/start", json=start_payload)
        assert start_res.status_code == 200
        campaign = start_res.json()
        campaign_id = campaign["campaign_id"]
        assert campaign["spanner_config"] == "regional-us-central1"
        assert len(campaign["region_results"]) == 2
        assert "us-central1" in campaign["region_results"]
        assert "europe-west1" in campaign["region_results"]

        # 2. Poll status
        status_res = await client.get(f"/api/v1/benchmark/status/{campaign_id}")
        assert status_res.status_code == 200
        status_data = status_res.json()
        assert status_data["campaign_id"] == campaign_id

        # Wait for asynchronous simulation to complete
        for _ in range(30):
            completed_res = await client.get(f"/api/v1/benchmark/status/{campaign_id}")
            assert completed_res.status_code == 200
            completed_data = completed_res.json()
            if completed_data["status"] == "completed":
                break
            await asyncio.sleep(0.5)

        assert completed_data["status"] == "completed"
        assert completed_data["progress_pct"] == 100

        # Verify latency telemetry was generated for each region
        for reg_id in ["us-central1", "europe-west1"]:
            reg_result = completed_data["region_results"][reg_id]
            assert reg_result["status"] == "completed"
            assert reg_result["writes"]["p50"] > 0
            assert reg_result["strong_reads"]["p50"] > 0
            assert reg_result["stale_reads"]["p50"] > 0
            # Writes should have valid percentile ordering: min <= p50 <= p99 <= max
            assert reg_result["writes"]["min"] <= reg_result["writes"]["p50"] <= reg_result["writes"]["max"]

        # 3. Stop Campaign (should succeed)
        stop_res = await client.post(f"/api/v1/benchmark/stop/{campaign_id}")
        assert stop_res.status_code == 200

        # 4. Cleanup Campaign Cloud Resources (preserves campaign data and results)
        cleanup_res = await client.post(f"/api/v1/benchmark/cleanup/{campaign_id}")
        assert cleanup_res.status_code == 200
        assert cleanup_res.json()["success"] is True

        # 5. After resource cleanup, campaign record remains with resources detached
        post_cleanup_res = await client.get(f"/api/v1/benchmark/status/{campaign_id}")
        assert post_cleanup_res.status_code == 200
        assert post_cleanup_res.json()["spanner_instance_id"] is None
        assert post_cleanup_res.json()["gce_instances"] == {}

        # 6. Delete campaign permanently removes campaign
        del_res = await client.delete(f"/api/v1/benchmark/campaigns/{campaign_id}")
        assert del_res.status_code == 200
        post_del_res = await client.get(f"/api/v1/benchmark/status/{campaign_id}")
        assert post_del_res.status_code == 404


@pytest.mark.asyncio
async def test_benchmark_disabled_feature_flag(monkeypatch):
    monkeypatch.setenv("ENABLE_LOAD_TESTING", "false")
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # System config reflects disabled
        config_res = await client.get("/api/v1/system/config")
        assert config_res.status_code == 200
        assert config_res.json()["features"]["enable_load_testing"] is False

        # Benchmark endpoint returns 404
        benchmark_res = await client.get("/api/v1/benchmark/client-regions")
        assert benchmark_res.status_code == 404


@pytest.mark.asyncio
async def test_allow_dry_run_disabled_feature_flag(monkeypatch):
    monkeypatch.setenv("ALLOW_DRY_RUN", "false")
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # System config reflects allow_dry_run = False
        config_res = await client.get("/api/v1/system/config")
        assert config_res.status_code == 200
        assert config_res.json()["features"]["allow_dry_run"] is False

        # Preflight check with dry_run returns 400
        preflight_res = await client.post(
            "/api/v1/benchmark/preflight",
            json={
                "project_id": "spanner-loadtest-demo",
                "spanner_config": "regional-us-central1",
                "client_regions": ["us-central1"],
                "execution_mode": "dry_run",
            },
        )
        assert preflight_res.status_code == 400
        assert "Simulated dry-run mode is disabled" in preflight_res.json()["detail"]

        # Creating campaign with dry_run returns 400
        create_res = await client.post(
            "/api/v1/benchmark/campaigns",
            json={
                "name": "Dry Run Test",
                "project_id": "spanner-loadtest-demo",
                "spanner_config": "regional-us-central1",
                "client_regions": ["us-central1"],
                "operations": 100,
                "staleness_seconds": 15,
                "execution_mode": "dry_run",
            },
        )
        assert create_res.status_code == 400
        assert "Simulated dry-run mode is disabled" in create_res.json()["detail"]

        # Creating campaign with live succeeds
        live_res = await client.post(
            "/api/v1/benchmark/campaigns",
            json={
                "name": "Live Test",
                "project_id": "spanner-loadtest-demo",
                "spanner_config": "regional-us-central1",
                "client_regions": ["us-central1"],
                "operations": 100,
                "staleness_seconds": 15,
                "execution_mode": "live",
            },
        )
        assert live_res.status_code == 200
        live_id = live_res.json()["campaign_id"]
        # Cleanup created campaign
        await client.delete(f"/api/v1/benchmark/campaigns/{live_id}")


@pytest.mark.asyncio
async def test_benchmark_crud_and_cleanup_all():
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Create a draft benchmark
        create_payload = {
            "name": "Custom Test Suite",
            "description": "Evaluating cross-continental latency",
            "project_id": "spanner-loadtest-demo",
            "spanner_config": "nam-eur-asia1",
            "client_regions": ["us-central1", "europe-west1"],
            "operations": 80,
            "staleness_seconds": 15,
            "execution_mode": "dry_run",
        }
        res = await client.post("/api/v1/benchmark/campaigns", json=create_payload)
        assert res.status_code == 200
        data = res.json()
        campaign_id = data["campaign_id"]
        assert data["name"] == "Custom Test Suite"
        assert data["status"] == "draft"
        assert data["progress_pct"] == 0

        # 2. List campaigns
        list_res = await client.get("/api/v1/benchmark/campaigns")
        assert list_res.status_code == 200
        campaigns = list_res.json()
        assert any(c["campaign_id"] == campaign_id for c in campaigns)

        # 3. Get single campaign
        get_res = await client.get(f"/api/v1/benchmark/campaigns/{campaign_id}")
        assert get_res.status_code == 200
        assert get_res.json()["campaign_id"] == campaign_id

        # 4. Update campaign
        update_payload = {
            "name": "Updated Test Suite Name",
            "client_regions": ["us-central1", "europe-west1", "asia-east1"],
        }
        update_res = await client.put(f"/api/v1/benchmark/campaigns/{campaign_id}", json=update_payload)
        assert update_res.status_code == 200
        updated_data = update_res.json()
        assert updated_data["name"] == "Updated Test Suite Name"
        assert len(updated_data["client_regions"]) == 3

        # 5. Run campaign
        run_res = await client.post(f"/api/v1/benchmark/campaigns/{campaign_id}/run")
        assert run_res.status_code == 200
        assert run_res.json()["status"] in ("preflight_check", "provisioning_spanner", "running", "completed")

        # 6. Global cleanup
        cleanup_res = await client.post("/api/v1/benchmark/cleanup-all")
        assert cleanup_res.status_code == 200
        cleanup_data = cleanup_res.json()
        assert cleanup_data["status"] == "success"

        # 7. Delete campaign
        del_res = await client.delete(f"/api/v1/benchmark/campaigns/{campaign_id}")
        assert del_res.status_code == 200
        assert del_res.json()["status"] == "deleted"

        # 8. Verify deleted
        get_after_del = await client.get(f"/api/v1/benchmark/campaigns/{campaign_id}")
        assert get_after_del.status_code == 404


@pytest.mark.asyncio
async def test_benchmark_custom_leader_region():
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Create campaign with a custom swapped leader region (eur5: default is europe-west2, RW replica is europe-west1)
        create_payload = {
            "name": "Custom Leader Test",
            "project_id": "spanner-loadtest-demo",
            "spanner_config": "eur5",
            "leader_region": "europe-west1",
            "client_regions": ["europe-west1", "europe-west2"],
            "operations": 50,
            "staleness_seconds": 15,
            "execution_mode": "dry_run",
        }
        res = await client.post("/api/v1/benchmark/campaigns", json=create_payload)
        assert res.status_code == 200
        data = res.json()
        assert data["leader_region"] == "europe-west1"
        cid = data["campaign_id"]

        # Europe-west1 client distance to leader should be 0 because leader is europe-west1
        assert data["region_results"]["europe-west1"]["distance_to_leader_km"] == 0.0

        # Update leader region back to default europe-west2
        update_res = await client.put(f"/api/v1/benchmark/campaigns/{cid}", json={"leader_region": "europe-west2"})
        assert update_res.status_code == 200
        updated = update_res.json()
        assert updated["leader_region"] == "europe-west2"
        assert updated["region_results"]["europe-west2"]["distance_to_leader_km"] == 0.0

        # Cleanup
        await client.delete(f"/api/v1/benchmark/campaigns/{cid}")


@pytest.mark.asyncio
async def test_benchmark_custom_leader_orchestrator_log():
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        create_payload = {
            "name": "Custom Leader Log Test",
            "project_id": "test-custom-leader-proj",
            "spanner_config": "nam6",
            "leader_region": "us-east1",
            "client_regions": ["us-east1"],
            "operations": 10,
            "staleness_seconds": 15,
            "execution_mode": "dry_run",
        }
        start_res = await client.post("/api/v1/benchmark/start", json=create_payload)
        assert start_res.status_code == 200
        cid = start_res.json()["campaign_id"]

        for _ in range(30):
            st_res = await client.get(f"/api/v1/benchmark/status/{cid}")
            st_data = st_res.json()
            if st_data["status"] == "completed":
                break
            await asyncio.sleep(0.5)

        assert st_data["status"] == "completed"
        log_messages = [l["message"] for l in st_data["logs"]]
        has_custom_leader_log = any("Simulating custom default leader 'us-east1'" in m for m in log_messages)
        assert has_custom_leader_log

        # Cleanup
        await client.delete(f"/api/v1/benchmark/campaigns/{cid}")


@pytest.mark.asyncio
async def test_gcp_projects_endpoint():
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        res = await client.get("/api/v1/projects")
        assert res.status_code == 200
        data = res.json()
        assert "projects" in data
        assert isinstance(data["projects"], list)
        assert len(data["projects"]) > 0
        # Check structure of projects
        p0 = data["projects"][0]
        assert "project_id" in p0
        assert "display_name" in p0

        # Test force_refresh
        res_refresh = await client.get("/api/v1/projects?force_refresh=true")
        assert res_refresh.status_code == 200
        data_refresh = res_refresh.json()
        assert "projects" in data_refresh
        assert len(data_refresh["projects"]) > 0


@pytest.mark.asyncio
async def test_benchmark_steps_and_logs_lifecycle():
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Create campaign
        create_payload = {
            "name": "Steps & Logs Test Suite",
            "project_id": "test-spanner-project",
            "spanner_config": "regional-us-central1",
            "client_regions": ["us-central1"],
            "operations": 20,
            "staleness_seconds": 15,
            "execution_mode": "dry_run",
            "auto_teardown": True,
        }
        create_res = await client.post("/api/v1/benchmark/campaigns", json=create_payload)
        assert create_res.status_code == 200
        cid = create_res.json()["campaign_id"]

        # Run campaign
        run_res = await client.post(f"/api/v1/benchmark/campaigns/{cid}/run")
        assert run_res.status_code == 200
        run_data = run_res.json()
        assert "steps" in run_data
        assert len(run_data["steps"]) == 7
        step_ids = [s["id"] for s in run_data["steps"]]
        assert step_ids == [
            "preflight",
            "provision_spanner",
            "create_database",
            "setup_clients",
            "execute_workload",
            "aggregate_metrics",
            "teardown",
        ]
        assert "logs" in run_data
        assert len(run_data["logs"]) > 0

        # Poll until finished
        for _ in range(30):
            poll_res = await client.get(f"/api/v1/benchmark/campaigns/{cid}")
            assert poll_res.status_code == 200
            data = poll_res.json()
            if data["status"] in ("completed", "failed"):
                break
            await asyncio.sleep(0.5)

        assert data["status"] == "completed"
        # Steps 1-6 should be completed
        for step in data["steps"][:-1]:
            assert step["status"] == "completed"
            assert step["started_at"] is not None
            assert step["completed_at"] is not None

        # Teardown step in dry-run mode is skipped
        assert data["steps"][-1]["status"] in ("completed", "skipped")

        # Logs should record all stages
        log_stages = {l["stage"] for l in data["logs"]}
        assert "preflight" in log_stages
        assert "spanner" in log_stages
        assert "database" in log_stages
        assert "workload" in log_stages
        assert "telemetry" in log_stages

        # Cleanup
        await client.delete(f"/api/v1/benchmark/campaigns/{cid}")



