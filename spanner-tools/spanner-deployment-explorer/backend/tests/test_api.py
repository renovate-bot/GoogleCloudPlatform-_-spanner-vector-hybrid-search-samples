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

import pytest
from starlette.testclient import TestClient
from app.main import app

@pytest.fixture
def client():
    return TestClient(app)

def test_health_check(client):
    for endpoint in ["/api/health", "/api/status", "/health", "/status"]:
        response = client.get(endpoint)
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "spanner-deployment-explorer"

def test_security_headers(client):
    response = client.get("/api/health")
    assert response.headers.get("x-content-type-options") == "nosniff"
    assert response.headers.get("x-frame-options") == "SAMEORIGIN"

def test_list_configs(client):
    response = client.get("/api/v1/configs")
    assert response.status_code == 200
    configs = response.json()
    assert len(configs) == 71
    assert any(c["configname"] == "eur3" for c in configs)
    assert any(c["configname"] == "us-central1" for c in configs)
    assert any(c["configname"] == "dual-region-canada1" for c in configs)
    assert not any(c["configname"] == "us-central2" and c["instancetype"] == "regional" for c in configs)

def test_filter_configs_by_continent(client):
    response = client.get("/api/v1/configs?continent=Europe")
    assert response.status_code == 200
    configs = response.json()
    assert len(configs) > 0
    assert all(c["continentregion"].lower() == "europe" for c in configs)

def test_filter_configs_by_instance_type(client):
    response = client.get("/api/v1/configs?instance_type=regional")
    assert response.status_code == 200
    configs = response.json()
    assert len(configs) > 0
    assert all(c["instancetype"] == "regional" for c in configs)

def test_search_configs(client):
    response = client.get("/api/v1/configs?search=tokyo")
    assert response.status_code == 200
    configs = response.json()
    assert len(configs) > 0
    assert any("tokyo" in c["display_name"].lower() or "asia-northeast1" in c["leader_region"] for c in configs)

def test_get_config_tree(client):
    response = client.get("/api/v1/configs/tree")
    assert response.status_code == 200
    tree = response.json()
    assert len(tree) > 0
    continents = [node["label"] for node in tree]
    assert "Europe" in continents
    assert "North America" in continents
    assert "Asia" in continents

def test_get_config_detail_multi_region(client):
    response = client.get("/api/v1/configs/eur3?nodes=2")
    assert response.status_code == 200
    detail = response.json()
    assert detail["configname"] == "eur3"
    assert detail["instancetype"] == "multi-region"
    assert detail["availability_sla"] == "99.999%"
    assert detail["nodes"] == 2
    assert len(detail["replicas"]) == 5
    # With 2 nodes: Leader (30k reads, 5.4k writes), RW (30k reads), Witness (0), 2 Optional Read-Only (2 * 30k = 60k reads)
    assert detail["total_reads_per_sec"] == 120000
    assert detail["total_writes_per_sec"] == 5400

def test_get_config_detail_nam3(client):
    response = client.get("/api/v1/configs/nam3?nodes=1")
    assert response.status_code == 200
    detail = response.json()
    assert detail["configname"] == "nam3"
    assert len(detail["replicas"]) == 15
    optional_replicas = [r for r in detail["replicas"] if r["replica_type"] == "optional read only"]
    assert len(optional_replicas) == 12
    assert any(r["region"] == "us-west8" for r in optional_replicas)
    assert any(r["region"] == "us-west2" for r in optional_replicas)

def test_get_config_detail_asia2(client):
    response = client.get("/api/v1/configs/asia2?nodes=1")
    assert response.status_code == 200
    detail = response.json()
    sg_rep = next(r for r in detail["replicas"] if r["region"] == "asia-southeast1")
    assert sg_rep["replica_type"] == "r/w replica"

def test_get_config_detail_regional(client):
    response = client.get("/api/v1/configs/us-central1?nodes=3")
    assert response.status_code == 200
    detail = response.json()
    assert detail["configname"] == "us-central1"
    assert detail["instancetype"] == "regional"
    assert detail["availability_sla"] == "99.99%"
    assert detail["nodes"] == 3
    assert len(detail["replicas"]) == 7
    # Regional with 3 nodes: 1 leader + 6 optional replicas = 7 * (22500 * 3) = 472500 reads, 3500 * 3 = 10500 writes
    assert detail["total_reads_per_sec"] == 472500
    assert detail["total_writes_per_sec"] == 10500

def test_visualize_deployments(client):
    payload = {
        "config_names": ["eur3", "asia1"],
        "nodes_map": {"eur3": 1, "asia1": 1},
    }
    response = client.post("/api/v1/deployments/visualize", json=payload)
    assert response.status_code == 200
    vis = response.json()
    assert len(vis["nodes"]) > 0
    assert len(vis["links"]) > 0
    assert vis["bounds"] is not None
    assert vis["total_reads_per_sec"] > 0
    assert vis["total_writes_per_sec"] > 0
    assert len(vis["configs"]) == 2

def test_export_cli(client):
    payload = {"configname": "eur3", "nodes": 3}
    response = client.post("/api/v1/export/cli", json=payload)
    assert response.status_code == 200
    export_data = response.json()
    assert "gcloud spanner instance-configs describe eur3" in export_data["gcloud_describe"]
    assert "--nodes=3" in export_data["gcloud_create"]
    assert 'resource "google_spanner_instance"' in export_data["terraform_hcl"]
    assert 'num_nodes    = 3' in export_data["terraform_hcl"]

def test_export_cli_custom_leader(client):
    # eur3 default leader is europe-west1, r/w replica is europe-west4
    payload = {"configname": "eur3", "nodes": 2, "leader_region": "europe-west4"}
    response = client.post("/api/v1/export/cli", json=payload)
    assert response.status_code == 200
    export_data = response.json()
    assert "default_leader = 'europe-west4'" in export_data["gcloud_create"]
    assert "google_spanner_database" in export_data["terraform_hcl"]
    assert "default_leader = 'europe-west4'" in export_data["terraform_hcl"]

def test_export_cli_regional(client):
    payload = {"configname": "europe-central2", "nodes": 1}
    response = client.post("/api/v1/export/cli", json=payload)
    assert response.status_code == 200
    export_data = response.json()
    assert "gcloud spanner instance-configs describe regional-europe-central2" in export_data["gcloud_describe"]
    assert "--config=regional-europe-central2" in export_data["gcloud_create"]
    assert 'instanceConfigs/regional-europe-central2' in export_data["terraform_hcl"]


def test_visualize_deployments_custom_leader(client):
    payload = {
        "config_names": ["eur3"],
        "nodes_map": {"eur3": 2},
        "leaders_map": {"eur3": "europe-west4"},
    }
    response = client.post("/api/v1/deployments/visualize", json=payload)
    assert response.status_code == 200
    vis = response.json()
    eur3_detail = next(c for c in vis["configs"] if c["configname"] == "eur3")
    assert eur3_detail["leader_region"] == "europe-west4"
    leader_rep = next(r for r in eur3_detail["replicas"] if r["region"] == "europe-west4")
    assert leader_rep["replica_type"] == "leader"
    assert leader_rep["writes_per_sec"] > 0
    former_leader_rep = next(r for r in eur3_detail["replicas"] if r["region"] == "europe-west1")
    assert former_leader_rep["replica_type"] == "r/w replica"
    assert former_leader_rep["writes_per_sec"] == 0

def test_not_found_config(client):
    response = client.get("/api/v1/configs/non-existent-config-xyz")
    assert response.status_code == 404


def test_visualize_edge_metrics(client):
    payload = {"config_names": ["eur3"]}
    response = client.post("/api/v1/deployments/visualize", json=payload)
    assert response.status_code == 200
    vis = response.json()
    links = vis["links"]
    assert len(links) > 0, "eur3 must have quorum links"

    for link in links:
        assert link["source_region"] is not None
        assert link["target_region"] is not None
        assert link["distance_km"] is not None and link["distance_km"] > 0
        assert link["distance_miles"] is not None and link["distance_miles"] > 0
        # Verify 'mi / km' formatting
        assert " mi / " in link["distance_label"]
        assert " km" in link["distance_label"]
        # Verify latency spectrum consistency
        assert link["latency_rtt_min_ms"] is not None
        assert link["latency_rtt_typical_ms"] is not None
        assert link["latency_rtt_max_ms"] is not None
        assert link["latency_rtt_min_ms"] <= link["latency_rtt_typical_ms"] <= link["latency_rtt_max_ms"]
        assert link["is_measured"] is True
        assert link["latency_label"].startswith("~")
        assert "ms" in link["latency_label"]
        assert "·" in link["latency_badge_text"]

    # Verify specific measured value for europe-west1 ↔ europe-west4 (7.79ms in heatmap.csv -> ~8ms)
    ew1_ew4_link = next(
        (l for l in links if {l["source_region"], l["target_region"]} == {"europe-west1", "europe-west4"}),
        None,
    )
    assert ew1_ew4_link is not None
    assert ew1_ew4_link["latency_label"] == "~8ms"
    assert ew1_ew4_link["latency_rtt_typical_ms"] == 8.0


def test_service_worker_unregistration_endpoint(client):
    """Verifies that /sw.js and /service-worker.js return self-unregistering script with no-cache headers."""
    for path in ["/sw.js", "/service-worker.js"]:
        response = client.get(path)
        assert response.status_code == 200
        assert "application/javascript" in response.headers["Content-Type"]
        assert "unregister" in response.text
        assert "no-cache" in response.headers.get("Cache-Control", "")


def test_latency_service_heatmap_lookup():
    """Validates that LatencyService parses heatmap.csv and rounds measured latencies to integer ms."""
    from app.services.latency_service import LatencyService

    service = LatencyService.get_instance()
    assert len(service._heatmap_lookup) >= 1800, f"Expected 1800+ pairs in heatmap, got {len(service._heatmap_lookup)}"

    # 1. Test user's specific example: us-east5 to asia-east1 (180.33ms -> ~180ms)
    metrics_us_asia = service.calculate_edge_metrics(
        0, 0, 0, 0, source_region="us-east5", target_region="asia-east1"
    )
    assert metrics_us_asia.is_measured is True
    assert metrics_us_asia.latency_rtt_typical_ms == 180.0
    assert metrics_us_asia.latency_label == "~180ms"

    # 2. Test intra-region sub-ms clamping: africa-south1 (0.58ms -> ~1ms)
    metrics_intra = service.calculate_edge_metrics(
        0, 0, 0, 0, source_region="africa-south1", target_region="africa-south1"
    )
    assert metrics_intra.is_measured is True
    assert metrics_intra.latency_rtt_typical_ms == 1.0
    assert metrics_intra.latency_label == "~1ms"

    # 3. Test short-haul: europe-west1 to europe-west2 (6.25ms -> ~6ms)
    metrics_ew1_ew2 = service.calculate_edge_metrics(
        0, 0, 0, 0, source_region="europe-west1", target_region="europe-west2"
    )
    assert metrics_ew1_ew2.is_measured is True
    assert metrics_ew1_ew2.latency_rtt_typical_ms == 6.0
    assert metrics_ew1_ew2.latency_label == "~6ms"

    # 4. Test newly added us-west8 to us-west4 (7.20ms -> ~7ms)
    metrics_usw8_usw4 = service.calculate_edge_metrics(
        0, 0, 0, 0, source_region="us-west8", target_region="us-west4"
    )
    assert metrics_usw8_usw4.is_measured is True
    assert metrics_usw8_usw4.latency_rtt_typical_ms == 7.0
    assert metrics_usw8_usw4.latency_label == "~7ms"

    # 4. Test unmapped region fallback (e.g. unknown region)
    metrics_fallback = service.calculate_edge_metrics(
        50.0, 4.0, 52.0, 5.0, source_region="unknown-reg-a", target_region="unknown-reg-b"
    )
    assert metrics_fallback.is_measured is False
    assert metrics_fallback.latency_label.startswith("~")
    assert metrics_fallback.latency_label.endswith("ms")


def test_visualize_with_client_regions(client):
    """Verify that visualize_deployments generates client_links with latency metrics."""
    response = client.post(
        "/api/v1/deployments/visualize",
        json={
            "config_names": ["nam3"],
            "client_regions": ["us-central1"],
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert "client_links" in data
    client_links = data["client_links"]
    assert len(client_links) == 15  # nam3 has 15 replicas (1 leader + 14 other replicas)

    leader_links = [l for l in client_links if l["link_type"] == "client_to_leader"]
    assert len(leader_links) == 1
    leader_link = leader_links[0]
    assert leader_link["source_region"] == "us-central1"
    assert leader_link["target_region"] == "us-east4"
    assert leader_link["latency_rtt_typical_ms"] is not None
    assert leader_link["latency_rtt_typical_ms"] > 0
    assert leader_link["latency_label"].startswith("~")
    assert "ms" in leader_link["latency_label"]
    assert leader_link["distance_miles"] is not None

    replica_links = [l for l in client_links if l["link_type"] == "client_to_replica"]
    assert len(replica_links) == 14
    for rep_link in replica_links:
        assert rep_link["source_region"] == "us-central1"
        assert rep_link["latency_rtt_typical_ms"] is not None
        assert rep_link["latency_label"] != ""


def test_visualize_without_client_regions(client):
    """Verify backward compatibility when client_regions is omitted."""
    response = client.post(
        "/api/v1/deployments/visualize",
        json={"config_names": ["nam3"]},
    )
    assert response.status_code == 200
    data = response.json()
    assert "client_links" in data
    assert data["client_links"] == []


def test_cli_export_with_optional_replicas(client):
    """Verify that export CLI generates custom instance config commands when optional replicas are selected."""
    response = client.post(
        "/api/v1/export/cli",
        json={
            "configname": "nam3",
            "nodes": 2,
            "optional_replicas": ["us-central1", "europe-west1"],
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert "gcloud_create" in data
    assert "terraform_hcl" in data
    assert "gcloud spanner instance-configs create custom-nam3-config" in data["gcloud_create"]
    assert "--clone-config=nam3" in data["gcloud_create"]
    assert "--add-replicas=location=us-central1,type=READ_ONLY" in data["gcloud_create"]
    assert "--add-replicas=location=europe-west1,type=READ_ONLY" in data["gcloud_create"]
    assert "--config=custom-nam3-config" in data["gcloud_create"]
    assert 'resource "google_spanner_instance_config" "custom_config"' in data["terraform_hcl"]
    assert "google_spanner_instance_config.custom_config.name" in data["terraform_hcl"]


def test_cli_export_without_optional_replicas(client):
    """Verify standard export CLI when no optional replicas are selected."""
    response = client.post(
        "/api/v1/export/cli",
        json={"configname": "nam3", "nodes": 1},
    )
    assert response.status_code == 200
    data = response.json()
    assert "gcloud spanner instance-configs create" not in data["gcloud_create"]
    assert "--config=nam3" in data["gcloud_create"]
    assert 'resource "google_spanner_instance_config"' not in data["terraform_hcl"]

