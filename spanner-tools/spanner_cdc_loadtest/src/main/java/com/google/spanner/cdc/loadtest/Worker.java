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

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Worker implements Callable<Void> {
  protected static final Logger logger = LoggerFactory.getLogger(Worker.class);
  protected final DatabaseClient dbClient;
  protected final String workerId;
  protected final AtomicLong opsCounter;

  public Worker(DatabaseClient dbClient, String workerId, AtomicLong opsCounter) {
    this.dbClient = dbClient;
    this.workerId = workerId;
    this.opsCounter = opsCounter;
  }

  @Override
  public Void call() {
    try {
      int ops = performOperation();
      opsCounter.addAndGet(ops);
    } catch (Exception e) {
      if (isInterrupted(e)) {
          logger.debug("Worker {} interrupted", workerId);
      } else {
          logger.error("Worker {} failed", workerId, e);
      }
    }
    return null;
  }
  
  protected boolean isInterrupted(Throwable e) {
      if (e instanceof InterruptedException) {
          return true;
      }
      if (e.getCause() != null && isInterrupted(e.getCause())) {
          return true;
      }
      // Check for Spanner retry exceptions causing interrupt
      String msg = e.getMessage();
      return msg != null && (msg.contains("InterruptedException") || msg.contains("interrupted"));
  }

  /**
   * Performs the operation and returns the number of logical operations (RPCs/Canaries) performed.
   */
  protected abstract int performOperation() throws Exception;
  
  // -------------------------------------------------------------------------------------------
  // Strategy: Random (Standard)
  // -------------------------------------------------------------------------------------------
  public static class RandomWorker extends Worker {
      public RandomWorker(DatabaseClient dbClient, String workerId, AtomicLong opsCounter) {
          super(dbClient, workerId, opsCounter);
      }

      @Override
      protected int performOperation() {
          // 40% Insert, 40% Update, 20% Delete
          double dice = Math.random();
          if (dice < 0.4) {
              insert();
          } else if (dice < 0.8) {
              update();
          } else {
              delete();
          }
          return 1;
      }
      
      private void insert() {
          String id = DataGenerator.generateId();
          dbClient.write(Arrays.asList(
              Mutation.newInsertBuilder("LoadTestTable")
                  .set("Id").to(id)
                  .set("Data").to(DataGenerator.generateData(100))
                  .set("Counter").to(DataGenerator.generateCounter())
                  .set("IsActive").to(DataGenerator.generateBool())
                  .set("ExampleTimestamp").to(com.google.cloud.Timestamp.now())
                  .build()
          ));
      }
      
      private void update() {
          String id = DataGenerator.generateId(); 
          
          dbClient.write(Arrays.asList(
              Mutation.newInsertOrUpdateBuilder("LoadTestTable")
                  .set("Id").to(id)
                  .set("Data").to(DataGenerator.generateData(100))
                  .set("Counter").to(DataGenerator.generateCounter())
                  .build()
          ));
      }
      
      private void delete() {
           String id = DataGenerator.generateId();
           dbClient.write(Arrays.asList(Mutation.delete("LoadTestTable", com.google.cloud.spanner.Key.of(id))));
      }
  }

  // -------------------------------------------------------------------------------------------
  // Strategy: Strict Ordering (Sequential)
  // -------------------------------------------------------------------------------------------
  public static class SequentialWorker extends Worker {
      public SequentialWorker(DatabaseClient dbClient, String workerId, AtomicLong opsCounter) {
          super(dbClient, workerId, opsCounter);
      }

      @Override
      protected int performOperation() {
          String id = DataGenerator.generateId(); // Unique ID for this sequence
          try {
             // INSERT
             dbClient.write(Arrays.asList(
                 Mutation.newInsertBuilder("LoadTestTable").set("Id").to(id).set("Counter").to(0).build()
             ));
             
             // Updates
             for (int i = 1; i <= 10; i++) {
                 dbClient.write(Arrays.asList(
                     Mutation.newUpdateBuilder("LoadTestTable").set("Id").to(id).set("Counter").to(i).build()
                 ));
             }
             
             // DELETE
             dbClient.write(Arrays.asList(Mutation.delete("LoadTestTable", com.google.cloud.spanner.Key.of(id))));
             
             return 12; 
          } catch (Exception e) {
              if (isInterrupted(e)) {
                  logger.debug("Sequential op interrupted");
              } else {
                  logger.warn("Sequential op failed", e);
              }
              return 0;
          }
      }
  }

  // -------------------------------------------------------------------------------------------
  // Strategy: Hotspot (High Contention)
  // -------------------------------------------------------------------------------------------
  public static class HotspotWorker extends Worker {
      private final String hotspotId;
      
      public HotspotWorker(DatabaseClient dbClient, String workerId, AtomicLong opsCounter, String hotspotId) {
          super(dbClient, workerId, opsCounter);
          this.hotspotId = hotspotId;
      }
      
      @Override
      protected int performOperation() {
          // Increment counter on the same ID
          TransactionRunner runner = dbClient.readWriteTransaction();
          try {
              runner.run(ctx -> {
                  // Read current
                  com.google.cloud.spanner.Struct row = ctx.readRow("LoadTestTable", com.google.cloud.spanner.Key.of(hotspotId), Arrays.asList("Counter"));
                  long val = 0;
                  if (row != null && !row.isNull(0)) {
                      val = row.getLong(0);
                  } else {
                      // If not exists, insert it
                      ctx.buffer(Mutation.newInsertOrUpdateBuilder("LoadTestTable")
                          .set("Id").to(hotspotId)
                          .set("Counter").to(0)
                          .build());
                      return null;
                  }
                  
                  ctx.buffer(Mutation.newUpdateBuilder("LoadTestTable")
                      .set("Id").to(hotspotId)
                      .set("Counter").to(val + 1)
                      .build());
                  return null;
              });
              return 1;
          } catch (Exception e) {
              if (!isInterrupted(e)) {
                  throw e;
              }
              return 0;
          }
      }
  }
  
  // -------------------------------------------------------------------------------------------
  // Strategy: Atomicity (Money Transfer)
  // -------------------------------------------------------------------------------------------
  public static class AtomicityWorker extends Worker {
      private final String id1;
      private final String id2;
      
      public AtomicityWorker(DatabaseClient dbClient, String workerId, AtomicLong opsCounter, String id1, String id2) {
          super(dbClient, workerId, opsCounter);
          this.id1 = id1;
          this.id2 = id2;
      }
      
      @Override
      protected int performOperation() {
         TransactionRunner runner = dbClient.readWriteTransaction();
         runner.run(ctx -> {
             // To simplify,  just blindly set values that sum to 100
             // Or we read and swap. Let's swap.
             long val1 = getCounterOrZero(ctx, id1);
             long val2 = getCounterOrZero(ctx, id2);
             
             // If they don't exist, init to 50/50
             if (val1 == -1 && val2 == -1) {
                 ctx.buffer(Mutation.newInsertOrUpdateBuilder("LoadTestTable").set("Id").to(id1).set("Counter").to(50).build());
                 ctx.buffer(Mutation.newInsertOrUpdateBuilder("LoadTestTable").set("Id").to(id2).set("Counter").to(50).build());
                 return null;
             }
             
             // Transfer 1 from id1 to id2
             ctx.buffer(Mutation.newUpdateBuilder("LoadTestTable").set("Id").to(id1).set("Counter").to(val1 - 1).build());
             ctx.buffer(Mutation.newUpdateBuilder("LoadTestTable").set("Id").to(id2).set("Counter").to(val2 + 1).build());
             return null;
         });
         return 1;
      }
      
      private long getCounterOrZero(TransactionContext ctx, String id) {
          com.google.cloud.spanner.Struct row = ctx.readRow("LoadTestTable", com.google.cloud.spanner.Key.of(id), Arrays.asList("Counter"));
          if (row == null) return -1;
          return row.isNull(0) ? 0 : row.getLong(0);
      }
  }

  // -------------------------------------------------------------------------------------------
  // Strategy: Saturation (Large Batches)
  // -------------------------------------------------------------------------------------------
  public static class SaturationWorker extends Worker {
       public SaturationWorker(DatabaseClient dbClient, String workerId, AtomicLong opsCounter) {
           super(dbClient, workerId, opsCounter);
       }
       
       @Override
       protected int performOperation() {
           // Large payload or batch
           List<Mutation> mutations = new ArrayList<>();
           // Batch of 100 large inserts
           for (int i = 0; i < 50; i++) {
               mutations.add(Mutation.newInsertBuilder("LoadTestTable")
                   .set("Id").to(DataGenerator.generateId())
                   .set("Data").to(DataGenerator.generateLargeData(20000)) // 20KB * 50 = ~1MB
                   .build());
           }
           dbClient.write(mutations);
           return 1;
       }
  }

  // -------------------------------------------------------------------------------------------
  // Strategy: Integrity (Resurrection & Tombstones)
  // -------------------------------------------------------------------------------------------
  public static class IntegrityWorker extends Worker {
      private final List<String> keyPool;
      private final java.util.Random random = new java.util.Random();

      public IntegrityWorker(DatabaseClient dbClient, String workerId, AtomicLong opsCounter, List<String> keyPool) {
          super(dbClient, workerId, opsCounter);
          this.keyPool = keyPool;
      }

      @Override
      protected int performOperation() {
          String key = keyPool.get(random.nextInt(keyPool.size()));
          // 50% Insert/Update, 50% Delete
          if (random.nextBoolean()) {
              // Insert or Update (Resurrection)
              dbClient.write(Arrays.asList(
                  Mutation.newInsertOrUpdateBuilder("LoadTestTable")
                      .set("Id").to(key)
                      .set("Data").to(DataGenerator.generateData(50))
                      .set("Counter").to(DataGenerator.generateCounter())
                      .set("IsActive").to(true)
                      .build()
              ));
          } else {
              // Delete
              dbClient.write(Arrays.asList(Mutation.delete("LoadTestTable", com.google.cloud.spanner.Key.of(key))));
          }
          return 1;
      }
  }

  // -------------------------------------------------------------------------------------------
  // Strategy: Read Heavy (90% Read / 10% Insert)
  // -------------------------------------------------------------------------------------------
  public static class ReadHeavyWorker extends Worker {
      private final List<String> knownIds = new ArrayList<>();
      private final java.util.Random random = new java.util.Random();
      private static final int MAX_CACHE_SIZE = 10000;

      public ReadHeavyWorker(DatabaseClient dbClient, String workerId, AtomicLong opsCounter) {
          super(dbClient, workerId, opsCounter);
      }

      @Override
      protected int performOperation() {
          // 90% Read, 10% Insert
          if (random.nextDouble() < 0.9) {
              performRead();
          } else {
              performInsert();
          }
          return 1;
      }

      private void performRead() {
          String id;
          if (knownIds.isEmpty()) {
              // Fallback if cache is empty
              id = DataGenerator.generateId();
          } else {
              // Pick random ID from cache
              id = knownIds.get(random.nextInt(knownIds.size()));
          }

          // Point lookup (Single Use Read)
          try (com.google.cloud.spanner.ResultSet rs = dbClient.singleUse().read("LoadTestTable",
                  com.google.cloud.spanner.KeySet.singleKey(com.google.cloud.spanner.Key.of(id)),
                  Arrays.asList("Id", "Data", "Counter"))) {
              while (rs.next()) {
                  // Consume result
                  rs.getString("Id");
              }
          }
      }

      private void performInsert() {
          String id = DataGenerator.generateId();
          
          dbClient.write(Arrays.asList(
              Mutation.newInsertBuilder("LoadTestTable")
                  .set("Id").to(id)
                  .set("Data").to(DataGenerator.generateData(100))
                  .set("Counter").to(DataGenerator.generateCounter())
                  .set("IsActive").to(true)
                  .set("ExampleTimestamp").to(com.google.cloud.Timestamp.now())
                  .build()
          ));

          // Valid ID, add to cache
          addToCache(id);
      }

      private void addToCache(String id) {
          if (knownIds.size() >= MAX_CACHE_SIZE) {
              knownIds.set(random.nextInt(knownIds.size()), id);
          } else {
              knownIds.add(id);
          }
      }
  }
}
