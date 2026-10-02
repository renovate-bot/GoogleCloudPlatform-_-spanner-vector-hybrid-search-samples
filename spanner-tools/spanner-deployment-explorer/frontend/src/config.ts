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

/**
 * UI Configuration Flags for Spanner Deployment Explorer.
 *
 * NOTE: Application feature flags and load test settings are centrally
 * managed in the root `config.json` file and dynamically loaded at runtime
 * via the `useAppConfig()` hook from `src/context/ConfigContext.tsx`.
 */

export { useAppConfig } from './context/ConfigContext';

// Backward-compatible fallback defaults
export const ALLOW_MULTIPLE_SELECTIONS = false;
export const ENABLE_LOAD_TESTING = true;
