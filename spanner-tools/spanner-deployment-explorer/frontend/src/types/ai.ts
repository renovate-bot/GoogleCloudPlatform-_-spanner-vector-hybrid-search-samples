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

export type AiRole = 'user' | 'assistant' | 'model';

export type AiActionType =
  | 'none'
  | 'update_sizing'
  | 'configure_benchmark'
  | 'configure_and_size';

export interface AiChatMessage {
  id: string;
  role: AiRole;
  content: string;
  timestamp: string;
  action?: AiActionType;
  spanner_config?: string;
  leader_region?: string;
  nodes?: number;
  client_regions?: string[];
  benchmark_name?: string;
  benchmark_description?: string;
  operations?: number;
  staleness_seconds?: number;
  optional_replicas?: string[];
}

export interface AiStatusResponse {
  enabled: boolean;
  has_key: boolean;
  model: string;
}

export interface AiChatRequest {
  messages: {
    role: string;
    content: string;
  }[];
  current_context?: {
    selected_config?: string;
    nodes?: number;
    leader_region?: string;
    selected_clients?: string[];
    is_multi_region?: boolean;
  };
}

export interface AiChatResponse {
  message: string;
  action?: AiActionType;
  spanner_config?: string;
  leader_region?: string;
  nodes?: number;
  client_regions?: string[];
  benchmark_name?: string;
  benchmark_description?: string;
  operations?: number;
  staleness_seconds?: number;
  optional_replicas?: string[];
}
