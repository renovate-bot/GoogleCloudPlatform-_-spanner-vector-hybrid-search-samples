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
