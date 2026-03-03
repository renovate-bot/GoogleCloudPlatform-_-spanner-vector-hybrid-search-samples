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

import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Schema {
  private static final Logger logger = LoggerFactory.getLogger(Schema.class);
  public static final String TABLE_NAME = "LoadTestTable";

  // Simplified DDL as requested
  private static final String CREATE_TABLE_DDL =
      "CREATE TABLE LoadTestTable ("
          + "    Id STRING(36) NOT NULL,"
          + "    Data STRING(MAX),"
          + "    Counter INT64,"
          + "    IsActive BOOL,"
          + "    ExampleTimestamp TIMESTAMP OPTIONS (allow_commit_timestamp=true),"
          + ") PRIMARY KEY (Id)";

  public static void createTableIfNotExists(
      Spanner spanner, String projectId, String instanceId, String databaseId) {
    DatabaseAdminClient dbAdminClient = spanner.getDatabaseAdminClient();
    DatabaseId db = DatabaseId.of(projectId, instanceId, databaseId);

    try {
      logger.info("Checking if table {} exists...", TABLE_NAME);
     
      dbAdminClient.updateDatabaseDdl(
          instanceId,
          databaseId,
          Collections.singletonList(CREATE_TABLE_DDL),
          null
      ).get();
      logger.info("Table {} created successfully.", TABLE_NAME);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof SpannerException) {
        SpannerException se = (SpannerException) e.getCause();
        if (se.getMessage().contains("Duplicate name")) {
          logger.info("Table {} already exists.", TABLE_NAME);
        } else {
          logger.error("Failed to create table", e);
          throw new RuntimeException("Failed to create table", e);
        }
      } else {
         logger.error("Failed to create table", e);
         throw new RuntimeException("Failed to create table", e);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while creating table", e);
    }
  }
}
