/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.geaflow.dsl.connector.iceberg;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.api.window.WindowType;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.Offset;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.TableSource;
import org.apache.geaflow.dsl.connector.api.serde.TableDeserializer;
import org.apache.geaflow.dsl.connector.api.window.FetchWindow;
import org.apache.iceberg.ScanTaskParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.data.Record;

/** Fixed snapshot source. Runtime caches are disposable; the JDBC plan and caller cursor are authoritative. */
public final class IcebergTableSource implements TableSource {
    private static final long serialVersionUID = 1L;

    private IcebergSourceOptions options;
    private TableSchema output;
    private ScanPlan.Root root;
    private transient IcebergCatalogSession session;
    private transient JdbcScanPlanStore store;
    private transient IcebergRecordMapper mapper;
    private transient Schema frozenSchema;
    private transient Schema projection;
    private transient Cursor cursor;
    private transient IcebergOffset cachedOffset;
    private transient long iteratorGeneration;

    @Override
    public void init(Configuration conf, TableSchema schema) {
        close();
        options = new IcebergSourceOptions(conf, schema.toString());
        output = schema;
        root = null;
    }

    @Override
    public void open(RuntimeContext context) {
        if (options == null) {
            throw new IllegalStateException("Initialize Iceberg source before open");
        }
        close();
        try {
            store = new JdbcScanPlanStore(options);
            ScanPlan.Root persisted = store.load();
            if (persisted == null && (root != null || Boolean.parseBoolean(options.get("resume", "false")))) {
                throw new IllegalStateException("Iceberg recovery plan is missing; refusing to reinterpret latest");
            }
            session = new IcebergCatalogSession(options);
            if (persisted == null) {
                persisted = store.publish(IcebergScanPlanner.plan(session, options, output));
            }
            if (root != null && !root.checksum.equals(persisted.checksum)) {
                throw new IllegalStateException("Iceberg recovery root was replaced");
            }
            root = persisted;
            root.validate(options);
            if (!root.tableUuid.equals(session.metadata().uuid())) {
                throw new IllegalStateException("Iceberg table identity changed");
            }
            if (root.snapshotId != null && session.table.snapshot(root.snapshotId) == null) {
                throw new IllegalStateException("Iceberg scan snapshot expired");
            }
            frozenSchema = SchemaParser.fromJson(root.tableSchema);
            projection = SchemaParser.fromJson(root.projection);
            mapper = new IcebergRecordMapper(output, projection);
        } catch (IOException | RuntimeException e) {
            try {
                close();
            } catch (RuntimeException closing) {
                e.addSuppressed(closing);
            }
            throw new IllegalStateException("Cannot open fixed Iceberg scan", e);
        }
    }

    @Override
    public List<Partition> listPartitions() {
        if (root == null) {
            throw new IllegalStateException("Plan Iceberg source before listing shards");
        }
        List<Partition> partitions = new ArrayList<>();
        for (int shard = 0; shard < root.shards; shard++) {
            partitions.add(new IcebergPartition(root.runKey, shard));
        }
        return Collections.unmodifiableList(partitions);
    }

    @Override
    public <IN> TableDeserializer<IN> getDeserializer(Configuration conf) {
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized <T> FetchData<T> fetch(Partition partition, Optional<Offset> offset, FetchWindow window) throws IOException {
        if (session == null) {
            throw new IllegalStateException("Iceberg source is closed");
        }
        if (window.getType() != WindowType.ALL_WINDOW && window.getType() != WindowType.SIZE_TUMBLING_WINDOW) {
            throw new IllegalArgumentException("Iceberg snapshot supports only ALL and SIZE windows");
        }
        if (window.getType() == WindowType.SIZE_TUMBLING_WINDOW && window.windowSize() <= 0) {
            throw new IllegalArgumentException("SIZE window must be positive");
        }
        if (!(partition instanceof IcebergPartition)) {
            throw new IllegalArgumentException("Foreign Iceberg partition");
        }
        IcebergPartition shard = (IcebergPartition) partition;
        if (shard.shard() < 0 || shard.shard() >= root.shards
            || !shard.equals(new IcebergPartition(root.runKey, shard.shard()))) {
            throw new IllegalArgumentException("Partition belongs to another Iceberg run");
        }
        if (offset.isPresent() && !(offset.get() instanceof IcebergOffset)) {
            throw new IllegalArgumentException("Foreign Iceberg offset");
        }
        IcebergOffset supplied = offset.isPresent() ? (IcebergOffset) offset.get() : null;
        try {
            if (cursor == null || supplied == null || !samePosition(cachedOffset, supplied)) {
                discardCursor();
                List<ScanPlan.Task> tasks = store.loadShard(root, shard.shard());
                if (supplied == null) {
                    supplied = new IcebergOffset(root.checksum, shard.shard(), 0,
                        tasks.isEmpty() ? null : tasks.get(0).key, 0, 0, window.windowId());
                }
                supplied.validate(root, shard.shard(), tasks, window.windowId());
                cursor = new Cursor(tasks, supplied);
            } else {
                supplied.validate(root, shard.shard(), cursor.tasks, window.windowId());
            }
            final long nextWindow = Math.addExact(window.windowId(), 1);
            if (window.getType() == WindowType.ALL_WINDOW) {
                final Cursor active = cursor;
                IcebergOffset end = new IcebergOffset(root.checksum, shard.shard(), active.tasks.size(), null, 0, -1, nextWindow);
                final long generation = ++iteratorGeneration;
                Iterator<Row> rows = new Iterator<Row>() {
                    private boolean exhausted;

                    @Override
                    public boolean hasNext() {
                        synchronized (IcebergTableSource.this) {
                            if (exhausted) {
                                return false;
                            }
                            if (generation != iteratorGeneration) {
                                throw new IllegalStateException("Iceberg iterator was superseded by recovery or close");
                            }
                            try {
                                if (active.ensure()) {
                                    return true;
                                }
                                exhausted = true;
                                return false;
                            } catch (IOException | RuntimeException e) {
                                fail(e);
                                throw new IllegalStateException("Iceberg ALL read failed", e);
                            }
                        }
                    }

                    @Override
                    public Row next() {
                        synchronized (IcebergTableSource.this) {
                            if (!hasNext()) {
                                throw new NoSuchElementException();
                            }
                            try {
                                return active.take();
                            } catch (RuntimeException e) {
                                fail(e);
                                throw e;
                            }
                        }
                    }
                };
                cachedOffset = null;
                return (FetchData<T>) FetchData.createBatchFetch(rows, end);
            }
            List<Row> rows = new ArrayList<>();
            long bytes = 0;
            while (rows.size() < window.windowSize() && cursor.ensure()) {
                long nextBytes = mapper.estimatedBytes(cursor.peek());
                if (nextBytes > options.maxBatchBytes) {
                    throw new IOException("Iceberg row exceeds batch memory budget");
                }
                if (bytes + nextBytes > options.maxBatchBytes) {
                    break;
                }
                rows.add(cursor.take());
                bytes += nextBytes;
            }
            boolean finished = !cursor.ensure();
            IcebergOffset next = cursor.offset(nextWindow);
            cachedOffset = next;
            return (FetchData<T>) FetchData.createStreamFetch(rows, next, finished);
        } catch (IOException | RuntimeException e) {
            fail(e);
            throw e;
        }
    }

    private static boolean samePosition(IcebergOffset left, IcebergOffset right) {
        return left != null && left.planChecksum.equals(right.planChecksum) && left.shard == right.shard
            && left.taskIndex == right.taskIndex && java.util.Objects.equals(left.taskKey, right.taskKey)
            && left.rowOrdinal == right.rowOrdinal && left.returnedRows == right.returnedRows && left.windowId == right.windowId;
    }

    private void fail(Exception original) {
        try {
            discardCursor();
        } catch (IOException closing) {
            original.addSuppressed(closing);
        }
    }

    private void discardCursor() throws IOException {
        final Cursor previous = cursor;
        cursor = null;
        cachedOffset = null;
        iteratorGeneration++;
        if (previous != null) {
            previous.closeReader();
        }
    }

    @Override
    public synchronized void close() {
        IOException failure = null;
        try {
            discardCursor();
        } catch (IOException e) {
            failure = e;
        }
        if (session != null) {
            try {
                session.close();
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            } finally {
                session = null;
            }
        }
        store = null;
        mapper = null;
        frozenSchema = null;
        projection = null;
        if (failure != null) {
            throw new UncheckedIOException(failure);
        }
    }

    private final class Cursor {
        private final List<ScanPlan.Task> tasks;
        private final int shard;
        private int index;
        private long ordinal;
        private long returned;
        private IcebergTaskReader reader;
        private Record pending;

        private Cursor(List<ScanPlan.Task> tasks, IcebergOffset offset) {
            this.tasks = tasks;
            shard = offset.shard;
            index = offset.taskIndex;
            ordinal = offset.rowOrdinal;
            returned = offset.returnedRows;
        }

        private boolean ensure() throws IOException {
            while (index < tasks.size()) {
                if (reader == null) {
                    reader = new IcebergTaskReader(session.table.io(), ScanTaskParser.fromJson(tasks.get(index).json, true),
                        frozenSchema, projection, root.nameMapping);
                    for (long skipped = 0; skipped < ordinal; skipped++) {
                        if (!reader.hasNext()) {
                            throw new IOException("Iceberg cursor points beyond visible task rows");
                        }
                        reader.next();
                    }
                }
                if (pending != null || reader.hasNext()) {
                    return true;
                }
                closeReader();
                index++;
                ordinal = 0;
            }
            return false;
        }

        private Record peek() {
            if (pending == null) {
                pending = reader.next();
            }
            return pending;
        }

        private Row take() {
            final Row row = mapper.map(peek());
            pending = null;
            ordinal = Math.addExact(ordinal, 1);
            returned = Math.addExact(returned, 1);
            return row;
        }

        private IcebergOffset offset(long nextWindow) {
            return new IcebergOffset(root.checksum, shard, index, index == tasks.size() ? null : tasks.get(index).key,
                ordinal, returned, nextWindow);
        }

        private void closeReader() throws IOException {
            IcebergTaskReader previous = reader;
            reader = null;
            pending = null;
            if (previous != null) {
                previous.close();
            }
        }
    }
}
