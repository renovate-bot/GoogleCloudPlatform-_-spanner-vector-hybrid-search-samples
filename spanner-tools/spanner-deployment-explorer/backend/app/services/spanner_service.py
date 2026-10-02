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

from collections import defaultdict
from typing import Dict, List, Optional, Tuple, Any

from app.models.spanner import (
    ReplicaType,
    InstanceType,
    ReplicaInfo,
    SpannerConfigSummary,
    SpannerConfigDetail,
    ConfigTreeNode,
    TopologyNode,
    TopologyLink,
    DeploymentVisualization,
    CliExportResponse,
)
from app.services.data_loader import DataLoader
from app.services.latency_service import LatencyService

def calculate_replica_throughput(
    replica_type: ReplicaType,
    instance_type: InstanceType,
    nodes: int = 1,
) -> Tuple[int, int]:
    """
    Calculates estimated (reads/sec, writes/sec) per replica based on Google Spanner documentation:
    - Multi-region / Dual-region:
        - leader: 15,000 reads/sec * nodes, 2,700 writes/sec * nodes
        - r/w replica: 15,000 reads/sec * nodes, 0 writes/sec
        - read only / optional read only: 15,000 reads/sec * nodes, 0 writes/sec
        - witness: 0 reads/sec, 0 writes/sec
    - Regional:
        - leader: 22,500 reads/sec * nodes, 3,500 writes/sec * nodes
    """
    if instance_type == InstanceType.REGIONAL:
        if replica_type == ReplicaType.LEADER:
            return 22500 * nodes, 3500 * nodes
        elif replica_type in (ReplicaType.READ_ONLY, ReplicaType.OPTIONAL_READ_ONLY):
            return 22500 * nodes, 0
        return 0, 0

    # Multi-region & Dual-region
    if replica_type == ReplicaType.LEADER:
        return 15000 * nodes, 2700 * nodes
    elif replica_type in (ReplicaType.RW_REPLICA, ReplicaType.READ_ONLY, ReplicaType.OPTIONAL_READ_ONLY):
        return 15000 * nodes, 0
    elif replica_type == ReplicaType.WITNESS:
        return 0, 0
    return 0, 0

class SpannerService:
    def __init__(self, data_loader: Optional[DataLoader] = None, latency_service: Optional[LatencyService] = None):
        self.loader = data_loader or DataLoader.get_instance()
        self.latency_service = latency_service or LatencyService.get_instance()

    def get_gcp_instance_config(self, configname: str) -> str:
        """Returns canonical GCP Spanner instanceConfig identifier (e.g. regional-europe-central2)."""
        return self.loader.get_gcp_instance_config(configname)

    def list_configs(
        self,
        continent: Optional[str] = None,
        instance_type: Optional[str] = None,
        search: Optional[str] = None,
    ) -> List[SpannerConfigSummary]:
        summaries: List[SpannerConfigSummary] = []

        for configname, rows in self.loader.configs_by_name.items():
            if not rows:
                continue

            first = rows[0]
            cfg_continent = first["continentregion"]
            cfg_inst_type = InstanceType(first["instancetype"])

            # Find leader region
            leader_row = next((r for r in rows if r["type"] == "leader"), first)
            leader_region = leader_row["region"]

            # Display name
            geo_info = self.loader.get_geo(leader_region)
            location_label = f" ({geo_info['locationname']})" if geo_info else ""
            display_name = f"{configname}{location_label}"

            # Filter checks
            if continent and cfg_continent.lower() != continent.lower():
                continue
            if instance_type and cfg_inst_type.value.lower() != instance_type.lower():
                continue
            if search:
                search_term = search.lower()
                matches_search = (
                    search_term in configname.lower()
                    or search_term in display_name.lower()
                    or search_term in cfg_continent.lower()
                    or any(search_term in r["region"].lower() for r in rows)
                )
                if not matches_search:
                    continue

            # SLA
            sla = "99.999%" if cfg_inst_type in (InstanceType.MULTI_REGION, InstanceType.DUAL_REGION) else "99.99%"

            # Base throughput with 1 node and replica list
            total_reads = 0
            total_writes = 0
            replicas_list: List[ReplicaInfo] = []
            for r in rows:
                rep_type = ReplicaType(r["type"])
                reads, writes = calculate_replica_throughput(rep_type, cfg_inst_type, 1)
                total_reads += reads
                total_writes += writes
                r_geo = self.loader.get_geo(r["region"])
                lat = r_geo["lat"] if r_geo else 0.0
                lon = r_geo["lon"] if r_geo else 0.0
                loc_name = r_geo["locationname"] if r_geo else r["region"]
                replicas_list.append(
                    ReplicaInfo(
                        region=r["region"],
                        location_name=loc_name,
                        lat=lat,
                        lon=lon,
                        replica_type=rep_type,
                        reads_per_sec=reads,
                        writes_per_sec=writes,
                        is_leader=(rep_type == ReplicaType.LEADER),
                        is_witness=(rep_type == ReplicaType.WITNESS),
                        is_read_only=(rep_type in (ReplicaType.READ_ONLY, ReplicaType.OPTIONAL_READ_ONLY)),
                    )
                )

            summaries.append(
                SpannerConfigSummary(
                    id=first["id"],
                    configname=configname,
                    display_name=display_name,
                    instancetype=cfg_inst_type,
                    continentregion=cfg_continent,
                    availability_sla=sla,
                    total_replicas=len(rows),
                    leader_region=leader_region,
                    nodes=1,
                    total_reads_per_sec=total_reads,
                    total_writes_per_sec=total_writes,
                    replicas=replicas_list,
                )
            )

        # Sort by continent then configname
        summaries.sort(key=lambda x: (x.continentregion, x.instancetype.value, x.configname))
        return summaries

    def get_config_tree(self) -> List[ConfigTreeNode]:
        """
        Builds hierarchical tree: Continent -> Instance Type -> Configuration
        """
        tree_map = defaultdict(lambda: defaultdict(list))

        for configname, rows in self.loader.configs_by_name.items():
            if not rows:
                continue
            first = rows[0]
            continent = first["continentregion"]
            inst_type = InstanceType(first["instancetype"])

            leader_row = next((r for r in rows if r["type"] == "leader"), first)
            geo = self.loader.get_geo(leader_row["region"])
            label = f"{configname} ({geo['locationname']})" if geo else configname

            tree_map[continent][inst_type].append({
                "configname": configname,
                "label": label,
                "replica_count": len(rows),
                "instancetype": inst_type,
            })

        result: List[ConfigTreeNode] = []
        for continent in sorted(tree_map.keys()):
            continent_children: List[ConfigTreeNode] = []
            continent_total_configs = 0

            for inst_type in sorted(tree_map[continent].keys(), key=lambda x: x.value):
                configs_list = tree_map[continent][inst_type]
                configs_list.sort(key=lambda x: x["configname"])
                continent_total_configs += len(configs_list)

                type_children = [
                    ConfigTreeNode(
                        key=f"{continent}-{inst_type.value}-{c['configname']}",
                        label=c["label"],
                        type="config",
                        configname=c["configname"],
                        instancetype=c["instancetype"],
                        replica_count=c["replica_count"],
                    )
                    for c in configs_list
                ]

                type_label = (
                    "Multi-Region" if inst_type == InstanceType.MULTI_REGION
                    else "Dual-Region" if inst_type == InstanceType.DUAL_REGION
                    else "Regional"
                )

                continent_children.append(
                    ConfigTreeNode(
                        key=f"{continent}-{inst_type.value}",
                        label=type_label,
                        count=len(type_children),
                        type="instancetype",
                        children=type_children,
                    )
                )

            result.append(
                ConfigTreeNode(
                    key=continent,
                    label=continent,
                    count=continent_total_configs,
                    type="continent",
                    children=continent_children,
                )
            )

        return result

    def get_config_detail(
        self,
        configname: str,
        nodes: int = 1,
        leader_region: Optional[str] = None,
    ) -> Optional[SpannerConfigDetail]:
        rows = self.loader.get_config(configname)
        if not rows:
            return None

        first = rows[0]
        cfg_continent = first["continentregion"]
        cfg_inst_type = InstanceType(first["instancetype"])
        sla = "99.999%" if cfg_inst_type in (InstanceType.MULTI_REGION, InstanceType.DUAL_REGION) else "99.99%"

        default_leader_row = next((r for r in rows if r["type"] == "leader"), first)
        default_leader_region = default_leader_row["region"]

        # If custom leader_region specified, verify it is an RW candidate
        if leader_region and any(r["region"] == leader_region and r["type"] in ("leader", "r/w replica") for r in rows):
            active_leader_region = leader_region
        else:
            active_leader_region = default_leader_region

        geo_info = self.loader.get_geo(active_leader_region)
        display_name = f"{configname} ({geo_info['locationname']})" if geo_info else configname

        replicas: List[ReplicaInfo] = []
        total_reads = 0
        total_writes = 0

        for r in rows:
            orig_type = r["type"]
            if orig_type in ("leader", "r/w replica"):
                rep_type = ReplicaType.LEADER if r["region"] == active_leader_region else ReplicaType.RW_REPLICA
            else:
                rep_type = ReplicaType(orig_type)

            r_geo = self.loader.get_geo(r["region"])
            lat = r_geo["lat"] if r_geo else 0.0
            lon = r_geo["lon"] if r_geo else 0.0
            loc_name = r_geo["locationname"] if r_geo else r["region"]

            reads, writes = calculate_replica_throughput(rep_type, cfg_inst_type, nodes)
            total_reads += reads
            total_writes += writes

            replicas.append(
                ReplicaInfo(
                    region=r["region"],
                    location_name=loc_name,
                    lat=lat,
                    lon=lon,
                    replica_type=rep_type,
                    reads_per_sec=reads,
                    writes_per_sec=writes,
                    is_leader=(rep_type == ReplicaType.LEADER),
                    is_witness=(rep_type == ReplicaType.WITNESS),
                    is_read_only=(rep_type in (ReplicaType.READ_ONLY, ReplicaType.OPTIONAL_READ_ONLY)),
                )
            )

        return SpannerConfigDetail(
            id=first["id"],
            configname=configname,
            display_name=display_name,
            instancetype=cfg_inst_type,
            continentregion=cfg_continent,
            availability_sla=sla,
            total_replicas=len(rows),
            leader_region=active_leader_region,
            nodes=nodes,
            total_reads_per_sec=total_reads,
            total_writes_per_sec=total_writes,
            replicas=replicas,
        )

    def visualize_deployments(
        self,
        config_names: List[str],
        nodes_map: Optional[Dict[str, int]] = None,
        leaders_map: Optional[Dict[str, str]] = None,
        client_regions: Optional[List[str]] = None,
    ) -> DeploymentVisualization:
        nodes_map = nodes_map or {}
        leaders_map = leaders_map or {}
        client_regions = client_regions or []
        nodes: List[TopologyNode] = []
        links: List[TopologyLink] = []
        client_links: List[TopologyLink] = []
        details: List[SpannerConfigDetail] = []
        placed_positions: Dict[Tuple[float, float], int] = defaultdict(int)

        agg_reads = 0
        agg_writes = 0
        all_lats: List[float] = []
        all_lons: List[float] = []

        for configname in config_names:
            config_nodes = nodes_map.get(configname, 1)
            custom_leader = leaders_map.get(configname)
            detail = self.get_config_detail(configname, config_nodes, custom_leader)
            if not detail:
                continue

            details.append(detail)
            agg_reads += detail.total_reads_per_sec
            agg_writes += detail.total_writes_per_sec

            leader_node_id: Optional[str] = None
            leader_coord: Optional[Tuple[float, float]] = None

            for rep in detail.replicas:
                base_lat = rep.lat
                base_lon = rep.lon

                # Apply slight offset if multiple nodes share exact lat/lon
                count_at_pos = placed_positions[(base_lat, base_lon)]
                placed_positions[(base_lat, base_lon)] += 1
                lat_offset = base_lat + (count_at_pos * 0.15)
                lon_offset = base_lon + (count_at_pos * 0.15)

                all_lats.append(lat_offset)
                all_lons.append(lon_offset)

                node_id = f"{configname}-{rep.region}-{rep.replica_type.value}"

                if rep.is_leader:
                    leader_node_id = node_id
                    leader_coord = (lat_offset, lon_offset)

                nodes.append(
                    TopologyNode(
                        id=node_id,
                        config_name=configname,
                        region=rep.region,
                        location_name=rep.location_name,
                        lat=lat_offset,
                        lon=lon_offset,
                        replica_type=rep.replica_type,
                        reads_per_sec=rep.reads_per_sec,
                        writes_per_sec=rep.writes_per_sec,
                        sla=detail.availability_sla,
                        nodes=config_nodes,
                    )
                )

            # Generate topology links from leader to replicas
            if leader_node_id and leader_coord and len(detail.replicas) > 1:
                for target_rep in detail.replicas:
                    if target_rep.is_leader:
                        continue

                    target_node_id = f"{configname}-{target_rep.region}-{target_rep.replica_type.value}"
                    target_coord = next(
                        ((n.lat, n.lon) for n in nodes if n.id == target_node_id),
                        (target_rep.lat, target_rep.lon),
                    )

                    link_type = "leader_to_rw"
                    if target_rep.replica_type == ReplicaType.WITNESS:
                        link_type = "leader_to_witness"
                    elif target_rep.replica_type in (ReplicaType.READ_ONLY, ReplicaType.OPTIONAL_READ_ONLY):
                        link_type = "leader_to_read_only"

                    edge_metrics = self.latency_service.calculate_edge_metrics(
                        leader_coord[0],
                        leader_coord[1],
                        target_coord[0],
                        target_coord[1],
                        source_region=detail.leader_region,
                        target_region=target_rep.region,
                    )

                    links.append(
                        TopologyLink(
                            source_id=leader_node_id,
                            target_id=target_node_id,
                            source_coord=leader_coord,
                            target_coord=target_coord,
                            link_type=link_type,
                            config_name=configname,
                            source_region=detail.leader_region,
                            target_region=target_rep.region,
                            distance_km=edge_metrics.distance_km,
                            distance_miles=edge_metrics.distance_miles,
                            distance_label=edge_metrics.distance_label,
                            latency_rtt_min_ms=edge_metrics.latency_rtt_min_ms,
                            latency_rtt_typical_ms=edge_metrics.latency_rtt_typical_ms,
                            latency_rtt_max_ms=edge_metrics.latency_rtt_max_ms,
                            latency_label=edge_metrics.latency_label,
                            latency_badge_text=edge_metrics.latency_badge_text,
                            is_measured=edge_metrics.is_measured,
                        )
                    )

                # Connect each R/W replica to the witness (consensus quorum participation)
                witness_reps = [r for r in detail.replicas if r.replica_type == ReplicaType.WITNESS]
                rw_reps = [r for r in detail.replicas if r.replica_type == ReplicaType.RW_REPLICA]

                for wit in witness_reps:
                    wit_node_id = f"{configname}-{wit.region}-{wit.replica_type.value}"
                    wit_coord = next(
                        ((n.lat, n.lon) for n in nodes if n.id == wit_node_id),
                        (wit.lat, wit.lon),
                    )
                    for rw in rw_reps:
                        rw_node_id = f"{configname}-{rw.region}-{rw.replica_type.value}"
                        rw_coord = next(
                            ((n.lat, n.lon) for n in nodes if n.id == rw_node_id),
                            (rw.lat, rw.lon),
                        )
                        wit_metrics = self.latency_service.calculate_edge_metrics(
                            rw_coord[0],
                            rw_coord[1],
                            wit_coord[0],
                            wit_coord[1],
                            source_region=rw.region,
                            target_region=wit.region,
                        )
                        links.append(
                            TopologyLink(
                                source_id=rw_node_id,
                                target_id=wit_node_id,
                                source_coord=rw_coord,
                                target_coord=wit_coord,
                                link_type="rw_to_witness",
                                config_name=configname,
                                source_region=rw.region,
                                target_region=wit.region,
                                distance_km=wit_metrics.distance_km,
                                distance_miles=wit_metrics.distance_miles,
                                distance_label=wit_metrics.distance_label,
                                latency_rtt_min_ms=wit_metrics.latency_rtt_min_ms,
                                latency_rtt_typical_ms=wit_metrics.latency_rtt_typical_ms,
                                latency_rtt_max_ms=wit_metrics.latency_rtt_max_ms,
                                latency_label=wit_metrics.latency_label,
                                latency_badge_text=wit_metrics.latency_badge_text,
                                is_measured=wit_metrics.is_measured,
                            )
                        )

        # Generate client links to Spanner instance replicas
        if client_regions:
            for configname in config_names:
                detail = next((d for d in details if d.configname == configname), None)
                if not detail or not detail.replicas:
                    continue

                leader_rep = next((r for r in detail.replicas if r.is_leader), detail.replicas[0])
                leader_node = next(
                    (n for n in nodes if n.config_name == configname and n.replica_type == ReplicaType.LEADER),
                    None,
                )
                leader_coord = (leader_node.lat, leader_node.lon) if leader_node else (leader_rep.lat, leader_rep.lon)
                leader_node_id = (
                    leader_node.id
                    if leader_node
                    else f"{configname}-{leader_rep.region}-{leader_rep.replica_type.value}"
                )

                for client_reg in client_regions:
                    client_geo = self.loader.geo_lookup.get(client_reg)
                    if not client_geo:
                        continue
                    client_coord = (client_geo["lat"], client_geo["lon"])

                    # 1. Client to Leader link
                    metrics_leader = self.latency_service.calculate_edge_metrics(
                        client_coord[0],
                        client_coord[1],
                        leader_coord[0],
                        leader_coord[1],
                        source_region=client_reg,
                        target_region=leader_rep.region,
                    )
                    client_links.append(
                        TopologyLink(
                            source_id=f"client-{client_reg}",
                            target_id=leader_node_id,
                            source_coord=client_coord,
                            target_coord=leader_coord,
                            link_type="client_to_leader",
                            config_name=configname,
                            source_region=client_reg,
                            target_region=leader_rep.region,
                            distance_km=metrics_leader.distance_km,
                            distance_miles=metrics_leader.distance_miles,
                            distance_label=metrics_leader.distance_label,
                            latency_rtt_min_ms=metrics_leader.latency_rtt_min_ms,
                            latency_rtt_typical_ms=metrics_leader.latency_rtt_typical_ms,
                            latency_rtt_max_ms=metrics_leader.latency_rtt_max_ms,
                            latency_label=metrics_leader.latency_label,
                            latency_badge_text=metrics_leader.latency_badge_text,
                            is_measured=metrics_leader.is_measured,
                        )
                    )

                    # 2. Client to other replicas (RW, Witness, Read-Only, Optional Read-Only)
                    for rep in detail.replicas:
                        if rep.is_leader:
                            continue
                        rep_node = next(
                            (
                                n
                                for n in nodes
                                if n.config_name == configname
                                and n.region == rep.region
                                and n.replica_type == rep.replica_type
                            ),
                            None,
                        )
                        rep_coord = (rep_node.lat, rep_node.lon) if rep_node else (rep.lat, rep.lon)
                        rep_node_id = (
                            rep_node.id
                            if rep_node
                            else f"{configname}-{rep.region}-{rep.replica_type.value}"
                        )

                        metrics_rep = self.latency_service.calculate_edge_metrics(
                            client_coord[0],
                            client_coord[1],
                            rep_coord[0],
                            rep_coord[1],
                            source_region=client_reg,
                            target_region=rep.region,
                        )
                        client_links.append(
                            TopologyLink(
                                source_id=f"client-{client_reg}",
                                target_id=rep_node_id,
                                source_coord=client_coord,
                                target_coord=rep_coord,
                                link_type="client_to_replica",
                                config_name=configname,
                                source_region=client_reg,
                                target_region=rep.region,
                                distance_km=metrics_rep.distance_km,
                                distance_miles=metrics_rep.distance_miles,
                                distance_label=metrics_rep.distance_label,
                                latency_rtt_min_ms=metrics_rep.latency_rtt_min_ms,
                                latency_rtt_typical_ms=metrics_rep.latency_rtt_typical_ms,
                                latency_rtt_max_ms=metrics_rep.latency_rtt_max_ms,
                                latency_label=metrics_rep.latency_label,
                                latency_badge_text=metrics_rep.latency_badge_text,
                                is_measured=metrics_rep.is_measured,
                            )
                        )

        # Compute bounding box
        bounds = None
        if all_lats and all_lons:
            min_lat = max(-85.0, min(all_lats) - 5.0)
            max_lat = min(85.0, max(all_lats) + 5.0)
            min_lon = max(-180.0, min(all_lons) - 5.0)
            max_lon = min(180.0, max(all_lons) + 5.0)
            bounds = ((min_lat, min_lon), (max_lat, max_lon))

        return DeploymentVisualization(
            nodes=nodes,
            links=links,
            client_links=client_links,
            bounds=bounds,
            total_reads_per_sec=agg_reads,
            total_writes_per_sec=agg_writes,
            configs=details,
        )

    def generate_cli_export(
        self,
        configname: str,
        nodes: int = 1,
        leader_region: Optional[str] = None,
        optional_replicas: Optional[List[str]] = None,
    ) -> CliExportResponse:
        detail = self.get_config_detail(configname, nodes, leader_region)
        display_name = detail.display_name if detail else configname
        actual_leader = detail.leader_region if detail else leader_region

        rows = self.loader.get_config(configname) or []
        default_leader_row = next((r for r in rows if r["type"] == "leader"), None)
        default_leader = default_leader_row["region"] if default_leader_row else ""

        has_custom_leader = bool(actual_leader and default_leader and actual_leader != default_leader)

        gcp_config = self.get_gcp_instance_config(configname)
        clean_opt_replicas = [r for r in (optional_replicas or []) if r]

        if clean_opt_replicas:
            custom_config_id = f"custom-{configname}-config"
            gcloud_describe = f"gcloud spanner instance-configs describe {custom_config_id}"

            replicas_flags = "\n".join(
                f"    --add-replicas=location={reg},type=READ_ONLY \\"
                for reg in clean_opt_replicas
            )
            gcloud_create = (
                f"# Step 1: Create custom instance configuration with {len(clean_opt_replicas)} optional read-only replica(s):\n"
                f"gcloud spanner instance-configs create {custom_config_id} \\\n"
                f"    --clone-config={gcp_config} \\\n"
                f"    --display-name=\"Custom {display_name} with Optional Replicas\" \\\n"
                f"{replicas_flags}\n"
                f"    --project=YOUR_PROJECT_ID\n\n"
                f"# Step 2: Create Spanner instance using custom instance config:\n"
                f"gcloud spanner instances create my-{configname}-instance \\\n"
                f"    --config={custom_config_id} \\\n"
                f"    --description=\"Spanner Instance ({display_name})\" \\\n"
                f"    --nodes={nodes}"
            )

            tf_replicas_entries = "\n".join(f'      "{reg}",' for reg in clean_opt_replicas)
            terraform_hcl = (
                f'# Custom Instance Configuration with Optional Read-Only Replicas\n'
                f'resource "google_spanner_instance_config" "custom_config" {{\n'
                f'  name         = "{custom_config_id}"\n'
                f'  display_name = "Custom {display_name} with Optional Replicas"\n'
                f'  base_config  = "projects/YOUR_PROJECT_ID/instanceConfigs/{gcp_config}"\n\n'
                f'  dynamic "replicas" {{\n'
                f'    for_each = [\n{tf_replicas_entries}\n    ]\n'
                f'    content {{\n'
                f'      location = replicas.value\n'
                f'      type     = "READ_ONLY"\n'
                f'    }}\n'
                f'  }}\n'
                f'}}\n\n'
                f'resource "google_spanner_instance" "spanner_instance" {{\n'
                f'  name         = "my-{configname}-instance"\n'
                f'  config       = google_spanner_instance_config.custom_config.name\n'
                f'  display_name = "Spanner Instance ({display_name})"\n'
                f'  num_nodes    = {nodes}\n'
                f'  labels = {{\n'
                f'    environment = "production"\n'
                f'    managed_by  = "terraform"\n'
                f'  }}\n'
                f'}}'
            )
        else:
            gcloud_describe = f"gcloud spanner instance-configs describe {gcp_config}"

            gcloud_create = (
                f"gcloud spanner instances create my-{configname}-instance \\\n"
                f"    --config={gcp_config} \\\n"
                f"    --description=\"Spanner Instance ({display_name})\" \\\n"
                f"    --nodes={nodes}"
            )

            terraform_hcl = (
                f'resource "google_spanner_instance" "spanner_instance" {{\n'
                f'  name         = "my-{configname}-instance"\n'
                f'  config       = "projects/YOUR_PROJECT_ID/instanceConfigs/{gcp_config}"\n'
                f'  display_name = "Spanner Instance ({display_name})"\n'
                f'  num_nodes    = {nodes}\n'
                f'  labels = {{\n'
                f'    environment = "production"\n'
                f'    managed_by  = "terraform"\n'
                f'  }}\n'
                f'}}'
            )

        if has_custom_leader:
            gcloud_create += (
                f"\n\n# Configure custom default leader on database:\n"
                f"gcloud spanner databases create my-database \\\n"
                f"    --instance=my-{configname}-instance\n\n"
                f"gcloud spanner databases ddl update my-database \\\n"
                f"    --instance=my-{configname}-instance \\\n"
                f"    --ddl=\"ALTER DATABASE \\`my-database\\` SET OPTIONS (default_leader = '{actual_leader}')\""
            )
            terraform_hcl += (
                f'\n\nresource "google_spanner_database" "spanner_database" {{\n'
                f'  instance = google_spanner_instance.spanner_instance.name\n'
                f'  name     = "my-database"\n'
                f'  ddl = [\n'
                f'    "ALTER DATABASE `my-database` SET OPTIONS (default_leader = \'{actual_leader}\')"\n'
                f'  ]\n'
                f'  deletion_protection = false\n'
                f'}}'
            )

        return CliExportResponse(
            configname=configname,
            gcloud_describe=gcloud_describe,
            gcloud_create=gcloud_create,
            terraform_hcl=terraform_hcl,
        )
