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

export type CheckStatus = 'passed' | 'failed' | 'warning' | 'checking';

export interface PreflightCheckItem {
  id: string;
  name: string;
  scope: string;
  status: CheckStatus;
  message: string;
  remediation_command?: string;
  documentation_url?: string;
}

export interface PreflightCheckResponse {
  all_passed: boolean;
  principal?: string;
  project_id: string;
  execution_mode: string;
  checks: PreflightCheckItem[];
  summary: string;
}

export interface LatencyMetrics {
  ops_count: number;
  hit_rate?: number;
  p50: number;
  p90: number;
  p95: number;
  p99: number;
  min: number;
  max: number;
  avg: number;
}

export interface RegionBenchmarkResult {
  region: string;
  region_name: string;
  distance_to_leader_km: number;
  has_local_replica: boolean;
  replica_type?: 'Leader Region' | 'R/W Replica Region' | 'Witness Region' | 'Read Only Region' | string | null;
  direct_access: boolean;
  direct_path_confirmed?: boolean | null;
  status: 'pending' | 'provisioning' | 'running' | 'completed' | 'stopped' | 'failed';
  progress_pct: number;
  writes: LatencyMetrics;
  strong_reads: LatencyMetrics;
  stale_reads: LatencyMetrics;
  error_message?: string;
}

export interface ProjectItem {
  project_id: string;
  display_name: string;
}

export interface ProjectsResponse {
  projects: ProjectItem[];
  current_active_project?: string;
}

export type StepStatus = 'pending' | 'running' | 'completed' | 'failed' | 'skipped';

export interface BenchmarkStep {
  id: string;
  name: string;
  description: string;
  status: StepStatus;
  started_at?: string;
  completed_at?: string;
  duration_seconds?: number;
  error_message?: string;
}

export interface BenchmarkLogEntry {
  timestamp: string;
  level: 'INFO' | 'WARN' | 'ERROR' | 'SUCCESS';
  stage: string;
  message: string;
}

export interface BenchmarkCampaignConfig {
  name?: string;
  description?: string;
  project_id: string;
  spanner_config: string;
  leader_region?: string;
  client_regions: string[];
  optional_replicas?: string[];
  operations: number;
  staleness_seconds: number;
  execution_mode: string;
  auto_teardown?: boolean;
  staging_bucket?: string;
}

export interface BenchmarkCampaignUpdate {
  name?: string;
  description?: string;
  project_id?: string;
  spanner_config?: string;
  leader_region?: string;
  client_regions?: string[];
  optional_replicas?: string[];
  operations?: number;
  staleness_seconds?: number;
  execution_mode?: string;
  auto_teardown?: boolean;
  staging_bucket?: string;
}

export type CampaignLifecycleState =
  | 'draft'
  | 'idle'
  | 'preflight_check'
  | 'provisioning_spanner'
  | 'provisioning_vms'
  | 'running'
  | 'completed'
  | 'stopped'
  | 'failed'
  | 'deleting';

export interface BenchmarkCampaignStatus {
  campaign_id: string;
  name?: string;
  description?: string;
  status: CampaignLifecycleState;
  progress_pct: number;
  project_id: string;
  spanner_instance_id?: string;
  spanner_database_id?: string;
  spanner_config: string;
  leader_region?: string;
  client_regions?: string[];
  optional_replicas?: string[];
  operations?: number;
  staleness_seconds?: number;
  message: string;
  execution_mode: string;
  region_results: Record<string, RegionBenchmarkResult>;
  created_at: string;
  completed_at?: string;
  auto_teardown?: boolean;
  staging_bucket?: string;
  gce_instances?: Record<string, string>;
  steps?: BenchmarkStep[];
  logs?: BenchmarkLogEntry[];
}

export interface ClientRegionInfo {
  region: string;
  name: string;
  continent: string;
  latitude: number;
  longitude: number;
}

export interface SystemFeatures {
  enable_load_testing: boolean;
  allow_dry_run: boolean;
  allow_multiple_selections: boolean;
  enable_edge_latency?: boolean;
  enable_ai?: boolean;
  has_ai_key?: boolean;
  ai_model?: string;
}

export interface SystemLoadTestingSettings {
  default_client_regions: string[];
  default_operations: number;
  default_staleness_seconds: number;
  gce_machine_type: string;
  spanner_nodes: number;
}

export interface SystemConfig {
  features: SystemFeatures;
  load_testing: SystemLoadTestingSettings;
}
