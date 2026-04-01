/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.spanner.benchmark.spanner;

import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.spanner.benchmark.config.AppConfig;

public final class SpannerClientFactory {
  private SpannerClientFactory() {}

  public static Spanner create(AppConfig config) {
    SpannerOptions.Builder builder =
        SpannerOptions.newBuilder()
            .setProjectId(config.projectId())
            .setNumChannels(4)
            .setEnableDirectAccess(config.directAccessEnabled())
            .setBuiltInMetricsEnabled(false);

    if (config.emulatorEnabled() && !config.endpoint().isBlank()) {
      builder.setEmulatorHost(normalizeEmulatorHost(config.endpoint()));
    } else if (!config.endpoint().isBlank()) {
      builder.setHost(config.endpoint());
    }

    return builder.build().getService();
  }

  private static String normalizeEmulatorHost(String endpoint) {
    return endpoint.replace("http://", "").replace("https://", "");
  }
}
