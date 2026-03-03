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
package com.google.spanner.cdc.loadtest;

import java.util.Random;
import java.util.UUID;
import org.apache.commons.lang3.RandomStringUtils;

public class DataGenerator {
  private static final Random random = new Random();

  public static String generateId() {
    return UUID.randomUUID().toString();
  }

  public static String generateData(int contentSize) {
    return RandomStringUtils.randomAlphanumeric(contentSize);
  }
  
  public static String generateLargeData(int sizeInBytes) {
      StringBuilder sb = new StringBuilder(sizeInBytes);
      String pattern = "AbCdEfGhIjKlMnOpQrStUvWxYz0123456789";
      for (int i = 0; i < sizeInBytes / pattern.length() + 1; i++) {
          sb.append(pattern);
      }
      return sb.substring(0, sizeInBytes);
  }

  public static long generateCounter() {
    return random.nextLong();
  }

  public static boolean generateBool() {
    return random.nextBoolean();
  }
  
  public static boolean shouldRun(double probability) {
      return random.nextDouble() < probability;
  }
}
