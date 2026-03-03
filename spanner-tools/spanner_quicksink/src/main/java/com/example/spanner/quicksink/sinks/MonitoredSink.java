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

import com.example.spanner.quicksink.util.Metrics;

public class MonitoredSink implements DataSink {
    
    private final DataSink delegate;
    
    public MonitoredSink(DataSink delegate) {
        this.delegate = delegate;
    }

    @Override
    public void write(SinkRecord record) {
        long start = System.currentTimeMillis();
        delegate.write(record);
        long end = System.currentTimeMillis();
        
        long processLag = end - record.getCaptureTimeMs();
        long dataLag = 0;
        
        if (record.isDataChange()) {
            String commitTsStr = record.getCommitTimestamp();
             if (commitTsStr != null && !commitTsStr.isEmpty()) {
                 try {
                    long commitTs = com.google.cloud.Timestamp.parseTimestamp(commitTsStr).toDate().getTime();
                    dataLag = end - commitTs;
                 } catch (Exception ignore) {}
             }
        }
        
        Metrics.markSinked(processLag, dataLag, record.getModCount());
    }
    
    @Override
    public void flush() {
        delegate.flush();
    }

    @Override
    public void close() {
        delegate.close();
    }
}
