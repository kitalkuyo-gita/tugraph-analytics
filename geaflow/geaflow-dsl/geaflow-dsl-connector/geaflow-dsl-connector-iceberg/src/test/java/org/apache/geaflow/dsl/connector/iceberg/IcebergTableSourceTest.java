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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.serialize.impl.KryoSerializer;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.Offset;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.TableConnector;
import org.apache.geaflow.dsl.connector.api.window.AllFetchWindow;
import org.apache.geaflow.dsl.connector.api.window.SizeFetchWindow;
import org.apache.geaflow.utils.keygroup.KeyGroup;
import org.apache.geaflow.utils.keygroup.KeyGroupAssignment;
import org.apache.iceberg.DataFile;
import org.testng.Assert;
import org.testng.annotations.Test;

public class IcebergTableSourceTest {
    @Test
    public void testSpi() {
        int count = 0;
        for (TableConnector connector : java.util.ServiceLoader.load(TableConnector.class)) {
            if (connector.getType().equalsIgnoreCase("iceberg")) {
                count++;
                Assert.assertTrue(connector instanceof IcebergTableConnector);
            }
        }
        Assert.assertEquals(count, 1);
    }

    @Test
    public void testParallelShardsWindowsAndDeletes() throws Exception {
        try (IcebergTestFixture fixture = new IcebergTestFixture()) {
            DataFile first = fixture.append(0, 21);
            fixture.append(21, 14);
            fixture.positionDelete(first, 5);
            fixture.equalityDelete(8L);
            // Same logical key inserted after the equality delete must remain visible.
            fixture.append(8, 1);
            List<Long> expected = new ArrayList<>();
            for (long i = 0; i < 35; i++) {
                if (i != 5) {
                    expected.add(i);
                }
            }
            for (int shards : new int[]{1, 7, 64}) {
                Configuration conf = fixture.configuration("shards-" + shards, shards);
                try (SourceHandle handle = open(conf)) {
                    List<Partition> partitions = handle.source.listPartitions();
                    Assert.assertEquals(new HashSet<>(partitions).size(), shards);
                    Assert.assertEquals(handle.source.listPartitions(99), partitions);
                    for (Partition partition : partitions) {
                        int hash = partition.hashCode();
                        partition.setIndex(5, 9);
                        Assert.assertEquals(partition.hashCode(), hash);
                    }
                    for (int parallelism : new int[]{1, 4, 8}) {
                        List<Partition> assigned = new ArrayList<>();
                        for (int worker = 0; worker < parallelism; worker++) {
                            KeyGroup group = KeyGroupAssignment.computeKeyGroupRangeForOperatorIndex(1024, parallelism, worker);
                            for (Partition partition : partitions) {
                                int id = KeyGroupAssignment.assignToKeyGroup(partition.getName(), 1024);
                                if (id >= group.getStartKeyGroup() && id <= group.getEndKeyGroup()) {
                                    assigned.add(partition);
                                }
                            }
                        }
                        Assert.assertEquals(assigned.size(), shards);
                        Assert.assertEquals(new HashSet<>(assigned), new HashSet<>(partitions));
                    }
                    Assert.assertEquals(read(handle.source, 4), expected);
                    Assert.assertEquals(read(handle.source, -1), expected);
                }
            }
        }
    }

    @Test
    public void testFixedSnapshotJavaKryoAndReconstruction() throws Exception {
        try (IcebergTestFixture fixture = new IcebergTestFixture()) {
            fixture.append(0, 12);
            Configuration conf = fixture.configuration("fixed", 1);
            IcebergTableSource source = new IcebergTableSource();
            source.init(conf, IcebergTestFixture.OUTPUT);
            source.open(null);
            Partition partition = source.listPartitions().get(0);
            FetchData<Row> first = source.fetch(partition, Optional.empty(), new SizeFetchWindow(1, 3));
            List<Long> firstRows = rows(first);
            IcebergOffset checkpoint = IcebergOffset.decode(((IcebergOffset) first.getNextOffset()).encode());
            source.close();
            source.close();
            fixture.append(100, 5);
            fixture.table.updateSchema().renameColumn("value", "renamed").commit();
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
                output.writeObject(source);
            }
            try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
                source = (IcebergTableSource) input.readObject();
            }
            source = (IcebergTableSource) new KryoSerializer().deserialize(new KryoSerializer().serialize(source));
            Assert.assertNotNull(source);
            source.open(null);
            try {
                FetchData<Row> second = source.fetch(partition, Optional.of(checkpoint), new SizeFetchWindow(2, 3));
                Assert.assertEquals(rows(second), java.util.Arrays.asList(3L, 4L, 5L));
                // Simulate collect failure: the caller retries exactly the same durable offset.
                Assert.assertEquals(rows(source.fetch(partition, Optional.of(checkpoint), new SizeFetchWindow(2, 3))), java.util.Arrays.asList(3L, 4L, 5L));
                Assert.assertEquals(firstRows, java.util.Arrays.asList(0L, 1L, 2L));
                IcebergTableSource restoredSource = source;
                expectFailure(() -> restoredSource.fetch(partition, Optional.of(checkpoint), new SizeFetchWindow(12, 3)));
            } finally {
                source.close();
            }
            // Rebuilt execution graph: no serialized source is needed if the stable run token survives.
            conf.put(IcebergSourceOptions.PREFIX + "resume", "true");
            try (SourceHandle handle = open(conf)) {
                Assert.assertEquals(read(handle.source, 5).size(), 12);
            }
        }
    }

    @Test
    public void testAllWindowRetryInvalidatesAbandonedIterator() throws Exception {
        try (IcebergTestFixture fixture = new IcebergTestFixture()) {
            fixture.append(0, 5);
            try (SourceHandle handle = open(fixture.configuration("all-retry", 1))) {
                Partition partition = handle.source.listPartitions().get(0);
                FetchData<Row> abandoned = handle.source.fetch(partition, Optional.empty(), new AllFetchWindow(1));
                Iterator<Row> old = abandoned.getDataIterator();
                old.next();
                old.next();
                // collect failed after two records; the durable window still has no offset.
                FetchData<Row> replay = handle.source.fetch(partition, Optional.empty(), new AllFetchWindow(1));
                expectFailure(old::hasNext);
                Assert.assertEquals(rows(replay), java.util.Arrays.asList(0L, 1L, 2L, 3L, 4L));
                Assert.assertTrue(replay.isFinish());
            }
        }
    }

    @Test
    public void testEmptyAndExactWindowBoundary() throws Exception {
        try (IcebergTestFixture fixture = new IcebergTestFixture()) {
            try (SourceHandle empty = open(fixture.configuration("empty", 7))) {
                fixture.append(0, 8);
                Assert.assertEquals(read(empty.source, 1).size(), 0);
            }
            for (int size : new int[]{1, 4, 8, 99}) {
                try (SourceHandle handle = open(fixture.configuration("boundary-" + size, 1))) {
                    Assert.assertEquals(read(handle.source, size), java.util.Arrays.asList(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L));
                }
            }
        }
    }

    @Test
    public void testUnprojectedDeleteKeyAndSelectors() throws Exception {
        try (IcebergTestFixture fixture = new IcebergTestFixture()) {
            fixture.append(0, 4);
            long snapshot = fixture.table.currentSnapshot().snapshotId();
            long timestamp = fixture.table.currentSnapshot().timestampMillis();
            fixture.table.manageSnapshots().createTag("before-delete", snapshot).commit();
            fixture.equalityDelete(2L);
            for (String selector : new String[]{"snapshot-id", "as-of-timestamp-ms", "ref"}) {
                Configuration conf = fixture.configuration(selector, 1);
                conf.put(IcebergSourceOptions.PREFIX + selector, selector.equals("ref") ? "before-delete"
                    : Long.toString(selector.equals("snapshot-id") ? snapshot : timestamp));
                try (SourceHandle handle = open(conf)) {
                    Assert.assertEquals(read(handle.source, -1), java.util.Arrays.asList(0L, 1L, 2L, 3L));
                }
            }
            IcebergTableSource source = new IcebergTableSource();
            source.init(fixture.configuration("projection", 1), new TableSchema(new TableField("value", Types.STRING)));
            source.open(null);
            try {
                FetchData<Row> data = source.fetch(source.listPartitions().get(0), Optional.empty(), new AllFetchWindow(1));
                List<String> values = new ArrayList<>();
                data.getDataIterator().forEachRemaining(row -> values.add((String) row.getField(0, Types.STRING)));
                Collections.sort(values);
                Assert.assertEquals(values, java.util.Arrays.asList("值-0", "值-1", "值-3"));
            } finally {
                source.close();
            }
        }
    }

    @Test
    public void testFailClosedOnConfigurationPlanAndIdentityChanges() throws Exception {
        try (IcebergTestFixture fixture = new IcebergTestFixture()) {
            fixture.append(0, 4);
            Configuration conf = fixture.configuration("fail-closed", 1);
            try (SourceHandle handle = open(conf)) {
                Assert.assertEquals(read(handle.source, -1).size(), 4);
            }
            Configuration changed = new Configuration(new java.util.HashMap<>(conf.getConfigMap()));
            changed.put(IcebergSourceOptions.PREFIX + "shard-count", "2");
            expectFailure(() -> open(changed));
            Configuration missing = fixture.configuration("missing", 1);
            missing.put(IcebergSourceOptions.PREFIX + "resume", "true");
            expectFailure(() -> open(missing));
            Configuration selector = fixture.configuration("selectors", 1);
            selector.put(IcebergSourceOptions.PREFIX + "snapshot-id", "1");
            selector.put(IcebergSourceOptions.PREFIX + "ref", "main");
            expectFailure(() -> open(selector));
            try (java.sql.Connection connection = java.sql.DriverManager.getConnection(fixture.jdbc);
                 java.sql.Statement statement = connection.createStatement()) {
                statement.executeUpdate("DELETE FROM geaflow_iceberg_tasks");
            }
            try (SourceHandle handle = open(conf)) {
                expectFailure(() -> read(handle.source, -1));
            }
            fixture.table.updateProperties().set("format-version", "3").commit();
            expectFailure(() -> open(fixture.configuration("v3", 1)));
        }
    }

    static List<Long> read(IcebergTableSource source, int size) throws Exception {
        List<Long> result = new ArrayList<>();
        for (Partition partition : source.listPartitions()) {
            Optional<Offset> offset = Optional.empty();
            long window = 1;
            while (true) {
                FetchData<Row> data = source.fetch(partition, offset, size < 0 ? new AllFetchWindow(window) : new SizeFetchWindow(window, size));
                result.addAll(rows(data));
                if (data.isFinish()) {
                    break;
                }
                offset = Optional.of(data.getNextOffset());
                if (++window > 10000) {
                    Assert.fail("Source did not finish");
                }
            }
        }
        Collections.sort(result);
        return result;
    }

    static List<Long> rows(FetchData<Row> data) {
        List<Long> values = new ArrayList<>();
        data.getDataIterator().forEachRemaining(row -> values.add((Long) row.getField(1, Types.LONG)));
        return values;
    }

    static SourceHandle open(Configuration conf) {
        IcebergTableSource source = new IcebergTableSource();
        source.init(conf, IcebergTestFixture.OUTPUT);
        source.open(null);
        return new SourceHandle(source);
    }

    static void expectFailure(ThrowingRunnable action) throws Exception {
        try {
            action.run();
            Assert.fail("Operation must fail closed");
        } catch (IllegalArgumentException | IllegalStateException | java.io.IOException expected) {
            // Expected explicit rejection; no fallback to latest or raw Parquet.
        }
    }

    interface ThrowingRunnable {
        void run() throws Exception;
    }

    static final class SourceHandle implements AutoCloseable {
        final IcebergTableSource source;

        SourceHandle(IcebergTableSource source) {
            this.source = source;
        }

        @Override
        public void close() {
            source.close();
        }
    }
}
