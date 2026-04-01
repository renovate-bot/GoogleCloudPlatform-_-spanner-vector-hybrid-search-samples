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
