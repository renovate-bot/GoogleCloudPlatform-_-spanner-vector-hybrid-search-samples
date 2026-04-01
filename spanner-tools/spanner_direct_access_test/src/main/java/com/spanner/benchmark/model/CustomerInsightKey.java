package com.spanner.benchmark.model;

import com.google.cloud.spanner.Key;

public record CustomerInsightKey(
    String custId,
    String acctNo,
    String phoneNumber,
    String insightCategory,
    String insightName) {

  public Key asSpannerKey() {
    return Key.of(custId, acctNo, phoneNumber, insightCategory, insightName);
  }
}
