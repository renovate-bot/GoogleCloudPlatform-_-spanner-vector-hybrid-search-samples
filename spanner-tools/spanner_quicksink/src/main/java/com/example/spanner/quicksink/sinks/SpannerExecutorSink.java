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

package com.example.spanner.quicksink.sinks;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.SpannerException;
import com.example.spanner.quicksink.util.Log;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class SpannerExecutorSink implements DataSink {

    private final DatabaseClient dbClient;
    private final int batchSize;
    private final int numThreads;
    private final List<List<Statement>> buffers;
    private final List<ExecutorService> executors;
    private final long flushTimeoutSeconds;
    private boolean closed = false;

    public SpannerExecutorSink(DatabaseClient dbClient, int batchSize) {
        this(dbClient, batchSize, 1, 60);
    }

    public SpannerExecutorSink(DatabaseClient dbClient, int batchSize, int numThreads) {
        this(dbClient, batchSize, numThreads, 60);
    }

    public SpannerExecutorSink(DatabaseClient dbClient, int batchSize, int numThreads, int flushTimeoutSeconds) {
        this.dbClient = dbClient;
        this.batchSize = batchSize;
        this.numThreads = Math.max(1, numThreads);
        this.flushTimeoutSeconds = flushTimeoutSeconds > 0 ? flushTimeoutSeconds : 60;
        
        
        this.buffers = new ArrayList<>(this.numThreads);
        this.executors = new ArrayList<>(this.numThreads);
        
        for (int i = 0; i < this.numThreads; i++) {
            this.buffers.add(new ArrayList<>());
            // Each shard has its own single-thread executor to ensure serial execution per shard
            this.executors.add(Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "SpannerSink-Shard-" + r.hashCode());
                t.setDaemon(true);
                return t;
            }));
        }
    }

    @Override
    public synchronized void write(SinkRecord record) {
        if (closed) return;

        Statement stmt = record.getStatement();
        if (stmt == null) {
            String data = record.getSql();
            if (data == null || data.isBlank()) return;
            
            // TODO: Verify if I really should strip trailing semicolon for SQLs 
            String sql = data.trim();
            if (sql.endsWith(";")) {
                sql = sql.substring(0, sql.length() - 1);
            }
            stmt = Statement.of(sql);
        }

        int shard = getShard(record.getShardKey());
        List<Statement> buffer = buffers.get(shard);
        buffer.add(stmt);

        if (buffer.size() >= batchSize) {
            submitBatch(shard);
        }
    }

    @Override
    public synchronized void flush() {
        if (closed) return;
        
        // Submit remaining buffers
        for (int i = 0; i < numThreads; i++) {
            if (!buffers.get(i).isEmpty()) {
                submitBatch(i);
            }
        }
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        try {
            flush();
            
            // Wait for all executors to finish their queues
            List<Future<?>> flushFutures = new ArrayList<>();
            for (int i = 0; i < numThreads; i++) {
                flushFutures.add(executors.get(i).submit(() -> { return null; }));
            }
            
            for (Future<?> f : flushFutures) {
                try {
                    f.get(flushTimeoutSeconds, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted during flush", e);
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof RuntimeException) throw (RuntimeException) cause;
                    throw new RuntimeException("Error during flush", cause);
                } catch (java.util.concurrent.TimeoutException e) {
                    f.cancel(true);
                    throw new RuntimeException("Timeout waiting for batch flush", e);
                }
            }
            
        } catch (Exception e) {
            Log.error("Error during final flush in close: " + e.getMessage());
        } finally {
            closed = true;
            for (ExecutorService exec : executors) {
                exec.shutdown();
                try {
                    if (!exec.awaitTermination(5, TimeUnit.SECONDS)) {
                        exec.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    exec.shutdownNow();
                }
            }
        }
    }

    private int getShard(String shardKey) {
        if (shardKey == null) return 0;
        // Use positive modulo
        return (shardKey.hashCode() & Integer.MAX_VALUE) % numThreads;
    }

    private void submitBatch(int shard) {
        List<Statement> buffer = buffers.get(shard);
        if (buffer.isEmpty()) return;

        final List<Statement> batch = new ArrayList<>(buffer);
        buffer.clear();
        ExecutorService exec = executors.get(shard);
        
        exec.submit(() -> writeBatch(batch));
    }

    private void writeBatch(List<Statement> batch) {
        try {
            Log.debug("[" + Thread.currentThread().getName() + "] Executing batch of " + batch.size() + " statements.");
            if (Log.isEnabled(Log.LEVEL_TRACE)) {
                for (Statement s : batch) {
                    Log.trace("  " + s.getSql());
                }
            }

            long[] updateCounts = dbClient.readWriteTransaction().run(tx -> {
                return tx.batchUpdate(batch);
            });
            long totalRows = 0;
            for (long count : updateCounts) {
                totalRows += count;
            }
            Log.debug("[" + Thread.currentThread().getName() + "] Executed batch of " + batch.size() + " statements. Total rows affected: " + totalRows);
        } catch (SpannerException e) {
            Log.error("Failed to execute batch: " + e.getMessage());
            throw new RuntimeException("Failed to execute batch", e);
        }
    }
}
