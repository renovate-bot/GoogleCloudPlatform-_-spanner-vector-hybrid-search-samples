// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

export type ReplicaType =
  | 'leader'
  | 'r/w replica'
  | 'witness'
  | 'optional read only'
  | 'read only';

export type InstanceType = 'regional' | 'dual-region' | 'multi-region';

export interface ReplicaInfo {
  region: string;
  location_name: string;
  lat: number;
  lon: number;
  replica_type: ReplicaType;
  reads_per_sec: number;
  writes_per_sec: number;
  is_leader: boolean;
  is_witness: boolean;
  is_read_only: boolean;
}

export interface SpannerConfig {
  id: string;
  configname: string;
  display_name: string;
  instancetype: InstanceType;
  continentregion: string;
  availability_sla: string;
  total_replicas: number;
  leader_region: string;
  replicas: ReplicaInfo[];
  total_reads_per_sec: number;
  total_writes_per_sec: number;
  nodes: number;
}

export interface ConfigTreeNode {
  key: string;
  label: string;
  count?: number;
  type?: 'continent' | 'instancetype' | 'config';
  children?: ConfigTreeNode[];
  configname?: string;
  instancetype?: InstanceType;
  replicaCount?: number;
}

export interface TopologyNode {
  id: string;
  config_name: string;
  region: string;
  location_name: string;
  lat: number;
  lon: number;
  replica_type: ReplicaType;
  reads_per_sec: number;
  writes_per_sec: number;
  sla: string;
  nodes: number;
}

export interface TopologyLink {
  source_id: string;
  target_id: string;
  source_coord: [number, number];
  target_coord: [number, number];
  link_type: 'leader_to_rw' | 'leader_to_witness' | 'rw_to_witness' | 'leader_to_read_only' | 'client_to_leader' | 'client_to_replica';
  config_name: string;
  source_region?: string;
  target_region?: string;
  distance_km?: number;
  distance_miles?: number;
  distance_label?: string;
  latency_rtt_min_ms?: number;
  latency_rtt_typical_ms?: number;
  latency_rtt_max_ms?: number;
  latency_label?: string;
  latency_badge_text?: string;
  is_measured?: boolean;
}

export interface DeploymentVisualization {
  nodes: TopologyNode[];
  links: TopologyLink[];
  client_links?: TopologyLink[];
  bounds?: [[number, number], [number, number]] | null;
  total_reads_per_sec: number;
  total_writes_per_sec: number;
  configs: SpannerConfig[];
}

export interface CliExportRequest {
  configname: string;
  nodes?: number;
  leader_region?: string;
  optional_replicas?: string[];
}

export interface CliExportResponse {
  configname: string;
  gcloud_describe: string;
  gcloud_create: string;
  terraform_hcl: string;
}

export interface ConfigFilterParams {
  continent?: string;
  instance_type?: InstanceType;
  search?: string;
}
