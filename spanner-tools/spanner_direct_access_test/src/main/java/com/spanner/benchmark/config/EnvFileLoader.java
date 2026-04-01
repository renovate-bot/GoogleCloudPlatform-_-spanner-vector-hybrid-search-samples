package com.spanner.benchmark.config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class EnvFileLoader {
  private EnvFileLoader() {}

  public static Map<String, String> load(Path envFile) {
    Map<String, String> values = new HashMap<>();
    if (!Files.exists(envFile)) {
      return values;
    }

    try {
      List<String> lines = Files.readAllLines(envFile);
      for (String rawLine : lines) {
        String line = rawLine.trim();
        if (line.isEmpty() || line.startsWith("#")) {
          continue;
        }

        int separator = line.indexOf('=');
        if (separator <= 0) {
          continue;
        }

        String key = line.substring(0, separator).trim();
        String value = stripQuotes(line.substring(separator + 1).trim());
        values.put(key, value);
      }
      return values;
    } catch (IOException e) {
      throw new IllegalStateException("Failed to read .env file: " + envFile, e);
    }
  }

  private static String stripQuotes(String value) {
    if (value.length() >= 2) {
      boolean doubleQuoted = value.startsWith("\"") && value.endsWith("\"");
      boolean singleQuoted = value.startsWith("'") && value.endsWith("'");
      if (doubleQuoted || singleQuoted) {
        return value.substring(1, value.length() - 1);
      }
    }
    return value;
  }
}
