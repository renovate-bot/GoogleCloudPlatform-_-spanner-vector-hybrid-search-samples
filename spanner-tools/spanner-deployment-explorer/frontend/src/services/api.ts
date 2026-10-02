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

import axios from 'axios';
import {
  SpannerConfig,
  ConfigTreeNode,
  DeploymentVisualization,
  CliExportResponse,
  ConfigFilterParams,
} from '../types/spanner';
import {
  BenchmarkCampaignConfig,
  BenchmarkCampaignStatus,
  BenchmarkCampaignUpdate,
  ClientRegionInfo,
  PreflightCheckResponse,
  ProjectsResponse,
  SystemConfig,
} from '../types/benchmark';
import {
  AiStatusResponse,
  AiChatResponse,
} from '../types/ai';

const apiClient = axios.create({
  baseURL: '/api/v1',
  headers: {
    'Content-Type': 'application/json',
  },
  timeout: 10000,
});

export const fetchConfigs = async (
  params?: ConfigFilterParams
): Promise<SpannerConfig[]> => {
  const response = await apiClient.get<SpannerConfig[]>('/configs', { params });
  return response.data;
};

export const fetchConfigTree = async (): Promise<ConfigTreeNode[]> => {
  const response = await apiClient.get<ConfigTreeNode[]>('/configs/tree');
  return response.data;
};

export const fetchConfigDetail = async (
  configname: string,
  nodes: number = 1
): Promise<SpannerConfig> => {
  const response = await apiClient.get<SpannerConfig>(
    `/configs/${encodeURIComponent(configname)}`,
    { params: { nodes } }
  );
  return response.data;
};

export const visualizeDeployments = async (
  confignames: string[],
  nodesMap: Record<string, number> = {},
  leadersMap: Record<string, string> = {},
  clientRegions: string[] = []
): Promise<DeploymentVisualization> => {
  const response = await apiClient.post<DeploymentVisualization>(
    '/deployments/visualize',
    {
      config_names: confignames,
      nodes_map: nodesMap,
      leaders_map: leadersMap,
      client_regions: clientRegions,
    }
  );
  return response.data;
};

export const exportCli = async (
  configname: string,
  nodes: number = 1,
  leaderRegion?: string,
  optionalReplicas?: string[]
): Promise<CliExportResponse> => {
  const response = await apiClient.post<CliExportResponse>('/export/cli', {
    configname,
    nodes,
    leader_region: leaderRegion,
    optional_replicas: optionalReplicas,
  });
  return response.data;
};

// System & Benchmark API Calls
export const fetchSystemConfig = async (): Promise<SystemConfig> => {
  const response = await apiClient.get<SystemConfig>('/system/config');
  return response.data;
};

export const fetchClientRegions = async (): Promise<ClientRegionInfo[]> => {
  const response = await apiClient.get<ClientRegionInfo[]>('/benchmark/client-regions');
  return response.data;
};

export const runPreflightCheck = async (params: {
  project_id: string;
  client_regions: string[];
  spanner_config: string;
  execution_mode: string;
}): Promise<PreflightCheckResponse> => {
  const response = await apiClient.post<PreflightCheckResponse>('/benchmark/preflight', params);
  return response.data;
};

export const startBenchmarkCampaign = async (
  config: BenchmarkCampaignConfig
): Promise<BenchmarkCampaignStatus> => {
  const response = await apiClient.post<BenchmarkCampaignStatus>('/benchmark/start', config);
  return response.data;
};

export const fetchCampaignStatus = async (
  campaignId: string
): Promise<BenchmarkCampaignStatus> => {
  const response = await apiClient.get<BenchmarkCampaignStatus>(`/benchmark/status/${campaignId}`);
  return response.data;
};

export const stopBenchmarkCampaign = async (
  campaignId: string
): Promise<{ status: string; campaign_id: string }> => {
  const response = await apiClient.post<{ status: string; campaign_id: string }>(
    `/benchmark/stop/${campaignId}`
  );
  return response.data;
};

export const cleanupCampaignResources = async (
  campaignId: string
): Promise<{ status: string; campaign_id: string; success: boolean }> => {
  const response = await apiClient.post<{ status: string; campaign_id: string; success: boolean }>(
    `/benchmark/cleanup/${campaignId}`,
    {},
    { timeout: 120000 }
  );
  return response.data;
};

export const fetchCampaigns = async (): Promise<BenchmarkCampaignStatus[]> => {
  const response = await apiClient.get<BenchmarkCampaignStatus[]>('/benchmark/campaigns');
  return response.data;
};

export const createCampaign = async (
  config: BenchmarkCampaignConfig
): Promise<BenchmarkCampaignStatus> => {
  const response = await apiClient.post<BenchmarkCampaignStatus>('/benchmark/campaigns', config);
  return response.data;
};

export const updateCampaign = async (
  campaignId: string,
  update: BenchmarkCampaignUpdate
): Promise<BenchmarkCampaignStatus> => {
  const response = await apiClient.put<BenchmarkCampaignStatus>(
    `/benchmark/campaigns/${campaignId}`,
    update
  );
  return response.data;
};

export const deleteCampaign = async (
  campaignId: string
): Promise<{ status: string; campaign_id: string; success: boolean }> => {
  const response = await apiClient.delete<{ status: string; campaign_id: string; success: boolean }>(
    `/benchmark/campaigns/${campaignId}`,
    { timeout: 120000 }
  );
  return response.data;
};

export const runCampaign = async (
  campaignId: string
): Promise<BenchmarkCampaignStatus> => {
  const response = await apiClient.post<BenchmarkCampaignStatus>(
    `/benchmark/campaigns/${campaignId}/run`,
    {},
    { timeout: 120000 }
  );
  return response.data;
};

export const cleanupAllResources = async (): Promise<{
  status: string;
  cleaned_campaigns_count: number;
  message: string;
}> => {
  const response = await apiClient.post<{
    status: string;
    cleaned_campaigns_count: number;
    message: string;
  }>('/benchmark/cleanup-all', {}, { timeout: 120000 });
  return response.data;
};

export const fetchGcpProjects = async (
  forceRefresh: boolean = false
): Promise<ProjectsResponse> => {
  const response = await apiClient.get<ProjectsResponse>('/projects', {
    params: { force_refresh: forceRefresh },
  });
  return response.data;
};

// ==========================================
// Spanner AI Assistant Endpoints
// ==========================================

export const fetchAiStatus = async (): Promise<AiStatusResponse> => {
  const response = await apiClient.get<AiStatusResponse>('/ai/status');
  return response.data;
};

export const saveAiKey = async (
  apiKey: string
): Promise<{ status: string; has_key: boolean; message: string }> => {
  const response = await apiClient.post<{ status: string; has_key: boolean; message: string }>(
    '/ai/key',
    { api_key: apiKey }
  );
  return response.data;
};

export const deleteAiKey = async (): Promise<{
  status: string;
  has_key: boolean;
  message: string;
}> => {
  const response = await apiClient.delete<{ status: string; has_key: boolean; message: string }>(
    '/ai/key'
  );
  return response.data;
};

export const sendAiChat = async (
  messages: { role: string; content: string }[],
  currentContext?: any
): Promise<AiChatResponse> => {
  const response = await apiClient.post<AiChatResponse>(
    '/ai/chat',
    {
      messages,
      current_context: currentContext,
    },
    { timeout: 35000 }
  );
  return response.data;
};

export const transcribeAudio = async (
  audioBase64: string,
  mimeType: string = 'audio/webm'
): Promise<{ text: string }> => {
  const response = await apiClient.post<{ text: string }>(
    '/ai/transcribe',
    {
      audio_base64: audioBase64,
      audio_mime_type: mimeType,
    },
    { timeout: 35000 }
  );
  return response.data;
};





