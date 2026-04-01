package com.spanner.benchmark.config;

import java.nio.file.Path;
import java.util.Map;

public record AppConfig(
    String projectId,
    String instanceId,
    String databaseId,
    String endpoint,
    boolean directAccessEnabled,
    boolean emulatorEnabled,
    String benchmarkAppName,
    int defaultWarmupIterations,
    int defaultMeasuredIterations,
    int defaultConcurrency,
    int defaultStalenessSeconds) {

  public static AppConfig load(Path repositoryRoot) {
    Map<String, String> fileValues = EnvFileLoader.load(repositoryRoot.resolve(".env"));
    return new AppConfig(
        required("GCP_PROJECT_ID", fileValues),
        required("SPANNER_INSTANCE_ID", fileValues),
        required("SPANNER_DATABASE_ID", fileValues),
        optional("SPANNER_ENDPOINT", fileValues, ""),
        requiredBoolean("GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS", fileValues),
        optionalBoolean("SPANNER_EMULATOR_ENABLED", fileValues, false),
        optional("BENCHMARK_APP_NAME", fileValues, "spanner-benchmark-poc"),
        optionalInt("BENCHMARK_DEFAULT_WARMUP_ITERATIONS", fileValues, 10),
        optionalInt("BENCHMARK_DEFAULT_MEASURED_ITERATIONS", fileValues, 100),
        optionalInt("BENCHMARK_DEFAULT_CONCURRENCY", fileValues, 1),
        optionalInt("BENCHMARK_DEFAULT_STALENESS_SECONDS", fileValues, 15));
  }

  private static String required(String key, Map<String, String> fileValues) {
    String value = firstNonBlank(System.getenv(key), fileValues.get(key));
    if (value == null) {
      throw new IllegalStateException("Missing required configuration: " + key);
    }
    return value;
  }

  private static String optional(String key, Map<String, String> fileValues, String defaultValue) {
    String value = firstNonBlank(System.getenv(key), fileValues.get(key));
    return value == null ? defaultValue : value;
  }

  private static boolean requiredBoolean(String key, Map<String, String> fileValues) {
    String value = firstNonBlank(System.getenv(key), fileValues.get(key));
    if (value == null) {
      throw new IllegalStateException("Missing required configuration: " + key);
    }
    return Boolean.parseBoolean(value);
  }

  private static int optionalInt(String key, Map<String, String> fileValues, int defaultValue) {
    String value = firstNonBlank(System.getenv(key), fileValues.get(key));
    return value == null ? defaultValue : Integer.parseInt(value);
  }

  private static boolean optionalBoolean(
      String key, Map<String, String> fileValues, boolean defaultValue) {
    String value = firstNonBlank(System.getenv(key), fileValues.get(key));
    return value == null ? defaultValue : Boolean.parseBoolean(value);
  }

  private static String firstNonBlank(String first, String second) {
    if (first != null && !first.isBlank()) {
      return first;
    }
    if (second != null && !second.isBlank()) {
      return second;
    }
    return null;
  }
}
