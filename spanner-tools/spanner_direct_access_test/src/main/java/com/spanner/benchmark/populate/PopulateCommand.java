package com.spanner.benchmark.populate;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.spanner.benchmark.config.AppConfig;
import com.spanner.benchmark.util.CliArguments;
import java.util.ArrayList;
import java.util.List;

public final class PopulateCommand {
  private static final List<String> TARGET_TABLES =
      List.of("customer_insights", "customer_insights_phone");

  private PopulateCommand() {}

  public static void run(AppConfig config, DatabaseClient client, String[] args) {
    CliArguments cli = CliArguments.parse(args);
    PopulationSpec spec = PopulationSpec.fromCli(cli);
    boolean truncateFirst = cli.has("truncate-first");

    long expectedRowsPerTable = spec.expectedRows();
    long totalExpectedRows = expectedRowsPerTable * TARGET_TABLES.size();
    System.out.printf(
        "Populating %,d rows across %d tables into %s/%s/%s%n",
        totalExpectedRows,
        TARGET_TABLES.size(),
        config.projectId(),
        config.instanceId(),
        config.databaseId());
    System.out.printf(
        "Profile=%s, customers=%d, customer-account pairs=%d, rows per customer-account=%d, rows per table=%d%n",
        spec.profileName(),
        spec.customers(),
        spec.customerAccountPairs(),
        spec.rowsPerCustomerAccount(),
        expectedRowsPerTable);
    System.out.printf(
        "Shape: accounts/customer=%d, phoneNumbers/account=%d, categories=%d, names/category=%d, batchSize=%d%n",
        spec.accountsPerCustomer(),
        spec.phoneNumbersPerAccount(),
        spec.categories(),
        spec.namesPerCategory(),
        spec.batchSize());

    if (truncateFirst) {
      System.out.printf("Truncating existing rows from %s before load%n", TARGET_TABLES);
      List<Mutation> deletions = new ArrayList<>(TARGET_TABLES.size());
      for (String tableName : TARGET_TABLES) {
        deletions.add(Mutation.delete(tableName, KeySet.all()));
      }
      client.write(deletions);
    }

    List<Mutation> batch = new ArrayList<>(spec.batchSize());
    long written = 0;
    for (int customer = 1; customer <= spec.customers(); customer++) {
      String custId = formatId("CUST", customer);
      for (int account = 1; account <= spec.accountsPerCustomer(); account++) {
        String acctNo = formatId("ACCT", customer, account);
        for (int phoneNumberIndex = 1;
            phoneNumberIndex <= spec.phoneNumbersPerAccount();
            phoneNumberIndex++) {
          String phoneNumber = formatPhoneNumber(customer, account, phoneNumberIndex);
          for (int category = 1; category <= spec.categories(); category++) {
            String insightCategory = "CATEGORY_" + String.format("%02d", category);
            for (int name = 1; name <= spec.namesPerCategory(); name++) {
              String insightName = "INSIGHT_" + String.format("%02d", name);
              for (String tableName : TARGET_TABLES) {
                batch.add(
                    Mutation.newInsertOrUpdateBuilder(tableName)
                        .set("cust_id")
                        .to(custId)
                        .set("acct_no")
                        .to(acctNo)
                        .set("phone_number")
                        .to(phoneNumber)
                        .set("insight_category")
                        .to(insightCategory)
                        .set("insight_name")
                        .to(insightName)
                        .set("insight_values")
                        .to(
                            makeInsightValue(
                                custId, acctNo, phoneNumber, insightCategory, insightName))
                        .set("updated_by")
                        .to(spec.updatedBy())
                        .set("updated_at")
                        .to(Value.COMMIT_TIMESTAMP)
                        .build());
              }

              if (batch.size() >= spec.batchSize()) {
                client.write(batch);
                written += batch.size();
                batch.clear();
                System.out.printf("Written %,d / %,d rows%n", written, totalExpectedRows);
              }
            }
          }
        }
      }
    }

    if (!batch.isEmpty()) {
      client.write(batch);
      written += batch.size();
      System.out.printf("Written %,d / %,d rows%n", written, totalExpectedRows);
    }

    System.out.printf("Populate complete. Final row count added: %,d%n", written);
  }

  private static String formatId(String prefix, int... parts) {
    StringBuilder builder = new StringBuilder(prefix);
    for (int part : parts) {
      builder.append('_').append(String.format("%03d", part));
    }
    return builder.toString();
  }

  private static String formatPhoneNumber(int customer, int account, int phoneNumberIndex) {
    return String.format("555%03d%03d%04d", customer, account, phoneNumberIndex);
  }

  private static String makeInsightValue(
      String custId,
      String acctNo,
      String phoneNumber,
      String insightCategory,
      String insightName) {
    return "{"
        + "\"custId\":\"" + custId + "\","
        + "\"acctNo\":\"" + acctNo + "\","
        + "\"phoneNumber\":\"" + phoneNumber + "\","
        + "\"category\":\"" + insightCategory + "\","
        + "\"name\":\"" + insightName + "\","
        + "\"score\":42,"
        + "\"segment\":\"baseline\""
        + "}";
  }

  private record PopulationSpec(
      String profileName,
      int customers,
      int accountsPerCustomer,
      int phoneNumbersPerAccount,
      int categories,
      int namesPerCategory,
      int batchSize,
      String updatedBy) {
    static PopulationSpec fromCli(CliArguments cli) {
      String profileName = cli.getString("profile", "small");
      PopulationSpec baseSpec = preset(profileName);
      return new PopulationSpec(
          profileName,
          cli.has("customers") ? cli.getInt("customers", baseSpec.customers()) : baseSpec.customers(),
          cli.has("accounts-per-customer")
              ? cli.getInt("accounts-per-customer", baseSpec.accountsPerCustomer())
              : baseSpec.accountsPerCustomer(),
          cli.has("phone-numbers-per-account")
              ? cli.getInt("phone-numbers-per-account", baseSpec.phoneNumbersPerAccount())
              : baseSpec.phoneNumbersPerAccount(),
          cli.has("categories")
              ? cli.getInt("categories", baseSpec.categories())
              : baseSpec.categories(),
          cli.has("names-per-category")
              ? cli.getInt("names-per-category", baseSpec.namesPerCategory())
              : baseSpec.namesPerCategory(),
          cli.has("batch-size") ? cli.getInt("batch-size", baseSpec.batchSize()) : baseSpec.batchSize(),
          cli.getString("updated-by", baseSpec.updatedBy()));
    }

    static PopulationSpec preset(String profileName) {
      return switch (profileName) {
        case "small" -> new PopulationSpec("small", 100, 2, 3, 3, 5, 500, "seed-loader");
        case "read-heavy-250" ->
            new PopulationSpec("read-heavy-250", 200, 5, 10, 5, 5, 1000, "seed-loader");
        case "read-heavy-1000" ->
            new PopulationSpec("read-heavy-1000", 200, 5, 20, 10, 5, 1000, "seed-loader");
        default ->
            throw new IllegalArgumentException(
                "Unsupported populate profile '"
                    + profileName
                    + "'. Supported values: small, read-heavy-250, read-heavy-1000");
      };
    }

    long expectedRows() {
      return (long) customers
          * accountsPerCustomer
          * phoneNumbersPerAccount
          * categories
          * namesPerCategory;
    }

    int rowsPerCustomerAccount() {
      return phoneNumbersPerAccount * categories * namesPerCategory;
    }

    int customerAccountPairs() {
      return customers * accountsPerCustomer;
    }
  }
}
