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

import React, { createContext, useContext, useEffect, useState } from 'react';
import { SystemConfig } from '../types/benchmark';
import { fetchSystemConfig } from '../services/api';

const DEFAULT_CONFIG: SystemConfig = {
  features: {
    enable_load_testing: true,
    allow_dry_run: true,
    allow_multiple_selections: false,
  },
  load_testing: {
    default_client_regions: [],
    default_operations: 1000,
    default_staleness_seconds: 15,
    gce_machine_type: 'e2-standard-2',
    spanner_nodes: 1,
  },
};

interface ConfigContextType {
  config: SystemConfig;
  loading: boolean;
  refreshConfig: () => Promise<void>;
}

const ConfigContext = createContext<ConfigContextType>({
  config: DEFAULT_CONFIG,
  loading: true,
  refreshConfig: async () => {},
});

export const ConfigProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [config, setConfig] = useState<SystemConfig>(DEFAULT_CONFIG);
  const [loading, setLoading] = useState<boolean>(true);

  const loadConfig = async () => {
    try {
      const data = await fetchSystemConfig();
      setConfig(data);
    } catch (err) {
      console.warn('⚠️ Could not load remote /system/config. Using default config.', err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadConfig();
  }, []);

  return (
    <ConfigContext.Provider value={{ config, loading, refreshConfig: loadConfig }}>
      {children}
    </ConfigContext.Provider>
  );
};

export const useAppConfig = () => {
  const context = useContext(ConfigContext);
  return {
    ...context.config,
    loading: context.loading,
    refreshConfig: context.refreshConfig,
  };
};
