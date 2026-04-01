package com.spanner.benchmark;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.spanner.benchmark.benchmark.BenchmarkCommand;
import com.spanner.benchmark.config.AppConfig;
import com.spanner.benchmark.populate.PopulateCommand;
import com.spanner.benchmark.spanner.SpannerClientFactory;
import java.nio.file.Path;

public final class Main {
  private Main() {}

  public static void main(String[] args) throws Exception {
    if (args.length == 0 || isHelp(args[0])) {
      printUsage();
      return;
    }

    AppConfig config = AppConfig.load(Path.of("."));
    try (Spanner spanner = SpannerClientFactory.create(config)) {
      DatabaseId databaseId =
          DatabaseId.of(config.projectId(), config.instanceId(), config.databaseId());
      DatabaseClient client = spanner.getDatabaseClient(databaseId);

      String command = args[0];
      String[] commandArgs = sliceArgs(args);
      switch (command) {
        case "populate" -> PopulateCommand.run(config, client, commandArgs);
        case "benchmark" -> BenchmarkCommand.run(config, client, commandArgs);
        default -> {
          System.err.printf("Unknown command: %s%n%n", command);
          printUsage();
          System.exit(1);
        }
      }
    }
  }

  private static boolean isHelp(String arg) {
    return "--help".equals(arg) || "-h".equals(arg) || "help".equals(arg);
  }

  private static String[] sliceArgs(String[] args) {
    String[] sliced = new String[Math.max(0, args.length - 1)];
    if (args.length > 1) {
      System.arraycopy(args, 1, sliced, 0, args.length - 1);
    }
    return sliced;
  }

  private static void printUsage() {
    System.out.println("""
        Usage:
          mvn exec:java -Dexec.args="populate [options]"
          mvn exec:java -Dexec.args="benchmark [options]"

        Commands:
          populate   Seed customer_insights and customer_insights_phone with synthetic data
          benchmark  Run focused read/query latency tests across one or both tables

        Populate options:
          --profile <name>            Default: small
          --truncate-first            Delete existing rows from both tables before loading
          --customers <n>             Default: 100
          --accounts-per-customer <n> Default: 2
          --phone-numbers-per-account <n> Default: 3
          --categories <n>            Default: 3
          --names-per-category <n>    Default: 5
          --batch-size <n>            Default: 500
          --updated-by <value>        Default: seed-loader

        Benchmark options:
          --table <name>              Default: all
          --scenario <name>           Default: core-read-paths
          --selection-mode <name>     Default: random
          --selection-seed <n>        Default: 42
          --warmup <n>                Default from .env or 10
          --iterations <n>            Default from .env or 100
          --concurrency <n>           Default from .env or 1
          --concurrency-sweep <list>  Example: 1,2,4,8
          --sample-size <n>           Default: 200
          --staleness-seconds <n>     Default from .env or 15
        """);
  }
}
