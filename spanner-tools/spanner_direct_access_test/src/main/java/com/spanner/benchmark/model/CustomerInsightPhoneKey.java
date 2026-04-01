package com.spanner.benchmark.model;

import com.google.cloud.spanner.Key;

public record CustomerInsightPhoneKey(
    String phoneNumber,
    String insightCategory,
    String insightName) {

  public Key asSpannerKey() {
    return Key.of(phoneNumber, insightCategory, insightName);
  }
}
