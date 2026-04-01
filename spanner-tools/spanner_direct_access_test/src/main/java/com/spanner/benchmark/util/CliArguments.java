package com.spanner.benchmark.util;

import java.util.HashMap;
import java.util.Map;

public final class CliArguments {
  private final Map<String, String> options;

  private CliArguments(Map<String, String> options) {
    this.options = options;
  }

  public static CliArguments parse(String[] args) {
    Map<String, String> options = new HashMap<>();
    for (int i = 0; i < args.length; i++) {
      String arg = args[i];
      if (!arg.startsWith("--")) {
        throw new IllegalArgumentException("Expected option starting with --, got: " + arg);
      }

      String key = arg.substring(2);
      if (i + 1 >= args.length || args[i + 1].startsWith("--")) {
        options.put(key, "true");
        continue;
      }

      options.put(key, args[++i]);
    }
    return new CliArguments(options);
  }

  public int getInt(String key, int defaultValue) {
    String value = options.get(key);
    return value == null ? defaultValue : Integer.parseInt(value);
  }

  public String getString(String key, String defaultValue) {
    return options.getOrDefault(key, defaultValue);
  }

  public boolean has(String key) {
    return options.containsKey(key);
  }
}
