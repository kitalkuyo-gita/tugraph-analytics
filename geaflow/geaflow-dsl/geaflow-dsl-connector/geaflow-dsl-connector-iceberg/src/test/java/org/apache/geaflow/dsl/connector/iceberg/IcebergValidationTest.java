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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.window.AllFetchWindow;
import org.apache.geaflow.dsl.connector.api.window.SizeFetchWindow;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.testng.Assert;
import org.testng.annotations.Test;

public class IcebergValidationTest {
    @Test
    public void testV1AndPartitionSpecEvolution() throws Exception {
        try (IcebergTestFixture fixture = new IcebergTestFixture(1)) {
            fixture.append(0, 3);
            fixture.table.updateSpec().addField("id").commit();
            GenericRecord record = GenericRecord.create(fixture.table.schema());
            record.setField("id", 9L);
            record.setField("value", "partitioned");
            append(fixture, record);
            try (IcebergTableSourceTest.SourceHandle handle = IcebergTableSourceTest.open(fixture.configuration("specs", 7))) {
                Assert.assertEquals(IcebergTableSourceTest.read(handle.source, 1), java.util.Arrays.asList(0L, 1L, 2L, 9L));
            }
        }
    }

    @Test
    public void testNullEqualityAndPrimitiveValues() throws Exception {
        try (IcebergTestFixture fixture = new IcebergTestFixture()) {
            fixture.table.updateSchema()
                .addColumn("flag", org.apache.iceberg.types.Types.BooleanType.get())
                .addColumn("amount", org.apache.iceberg.types.Types.DecimalType.of(38, 9))
                .addColumn("day", org.apache.iceberg.types.Types.DateType.get())
                .addColumn("ratio", org.apache.iceberg.types.Types.DoubleType.get()).commit();
            GenericRecord nullKey = GenericRecord.create(fixture.table.schema());
            nullKey.setField("value", "delete null key");
            append(fixture, nullKey);
            fixture.equalityDelete(null);
            GenericRecord row = GenericRecord.create(fixture.table.schema());
            row.setField("id", 1L);
            row.setField("value", "中文🚀");
            row.setField("flag", true);
            row.setField("amount", new java.math.BigDecimal("12345678901234567890123456789.123456789"));
            row.setField("day", java.time.LocalDate.of(1960, 1, 2));
            row.setField("ratio", Double.NaN);
            append(fixture, row);
            IcebergTableSource source = new IcebergTableSource();
            source.init(fixture.configuration("types", 1), new TableSchema(
                new TableField("amount", Types.DECIMAL), new TableField("day", Types.DATE),
                new TableField("value", Types.BINARY_STRING), new TableField("flag", Types.BOOLEAN),
                new TableField("ratio", Types.DOUBLE)));
            source.open(null);
            try {
                FetchData<Row> data = source.fetch(source.listPartitions().get(0), Optional.empty(), new AllFetchWindow(1));
                List<Row> values = new ArrayList<>();
                data.getDataIterator().forEachRemaining(values::add);
                Assert.assertEquals(values.size(), 1);
                Row value = values.get(0);
                Assert.assertEquals(value.getField(0, Types.DECIMAL), row.getField("amount"));
                Assert.assertEquals(value.getField(1, Types.DATE).toString(), "1960-01-02");
                Assert.assertEquals(value.getField(2, Types.BINARY_STRING).toString(), "中文🚀");
                Assert.assertEquals(value.getField(3, Types.BOOLEAN), true);
                Assert.assertTrue(Double.isNaN((Double) value.getField(4, Types.DOUBLE)));
            } finally {
                source.close();
            }
        }
    }

    @Test
    public void testMemoryLimitAndOversizedRow() throws Exception {
        try (IcebergTestFixture fixture = new IcebergTestFixture()) {
            for (int i = 0; i < 3; i++) {
                GenericRecord row = GenericRecord.create(fixture.table.schema());
                row.setField("id", (long) i);
                row.setField("value", "x".repeat(300));
                append(fixture, row);
            }
            Configuration conf = fixture.configuration("budget", 1);
            conf.put(IcebergSourceOptions.PREFIX + "batch.max-bytes", "1024");
            try (IcebergTableSourceTest.SourceHandle handle = IcebergTableSourceTest.open(conf)) {
                Assert.assertEquals(IcebergTableSourceTest.read(handle.source, 100), java.util.Arrays.asList(0L, 1L, 2L));
            }
            GenericRecord large = GenericRecord.create(fixture.table.schema());
            large.setField("id", 99L);
            large.setField("value", "x".repeat(1024));
            append(fixture, large);
            conf.put(IcebergSourceOptions.PREFIX + "run-id", "oversized");
            try (IcebergTableSourceTest.SourceHandle handle = IcebergTableSourceTest.open(conf)) {
                IcebergTableSourceTest.expectFailure(() -> IcebergTableSourceTest.read(handle.source, 100));
            }
        }
    }

    @Test
    public void testInvalidCursorCanBeRetriedAndCodecRejectsUnknownVersion() throws Exception {
        try (IcebergTestFixture fixture = new IcebergTestFixture()) {
            fixture.append(0, 5);
            try (IcebergTableSourceTest.SourceHandle handle = IcebergTableSourceTest.open(fixture.configuration("cursor", 1))) {
                Partition partition = handle.source.listPartitions().get(0);
                FetchData<Row> data = handle.source.fetch(partition, Optional.empty(), new SizeFetchWindow(1, 2));
                IcebergOffset valid = (IcebergOffset) data.getNextOffset();
                IcebergOffset invalid = new IcebergOffset(valid.planChecksum, valid.shard, valid.taskIndex,
                    valid.taskKey, 999999, valid.returnedRows, 2);
                IcebergTableSourceTest.expectFailure(() -> handle.source.fetch(partition, Optional.of(invalid), new SizeFetchWindow(2, 2)));
                Assert.assertEquals(IcebergTableSourceTest.rows(handle.source.fetch(partition, Optional.of(valid), new SizeFetchWindow(2, 2))),
                    java.util.Arrays.asList(2L, 3L));
                byte[] bytes = valid.encode();
                bytes[3] = 99;
                IcebergTableSourceTest.expectFailure(() -> IcebergOffset.decode(bytes));
                IcebergTableSourceTest.expectFailure(() -> handle.source.fetch(partition, Optional.empty(), new SizeFetchWindow(1, 0)));
            }
        }
    }

    @Test
    public void testMissingRootCorruptionAndRecreatedTable() throws Exception {
        try (IcebergTestFixture fixture = new IcebergTestFixture()) {
            fixture.append(0, 3);
            Configuration conf = fixture.configuration("root", 1);
            IcebergTableSourceTest.SourceHandle handle = IcebergTableSourceTest.open(conf);
            handle.close();
            try (Connection connection = DriverManager.getConnection(fixture.jdbc); Statement statement = connection.createStatement()) {
                statement.executeUpdate("UPDATE geaflow_iceberg_tasks SET payload = '{}' ");
            }
            handle.source.open(null);
            try {
                IcebergTableSourceTest.expectFailure(() -> IcebergTableSourceTest.read(handle.source, -1));
            } finally {
                handle.close();
            }
            try (Connection connection = DriverManager.getConnection(fixture.jdbc); Statement statement = connection.createStatement()) {
                statement.executeUpdate("DELETE FROM geaflow_iceberg_tasks");
                statement.executeUpdate("DELETE FROM geaflow_iceberg_runs");
            }
            IcebergTableSourceTest.expectFailure(() -> handle.source.open(null));
            try (IcebergTableSourceTest.SourceHandle replacement = IcebergTableSourceTest.open(fixture.configuration("identity", 1))) {
                replacement.close();
                fixture.catalog.dropTable(TableIdentifier.of("db", "records"), false);
                fixture.catalog.createTable(TableIdentifier.of("db", "records"), IcebergTestFixture.SCHEMA);
                IcebergTableSourceTest.expectFailure(() -> replacement.source.open(null));
            }
        }
    }

    @Test
    public void testRejectsUnknownOptionsSecretsAndTimestamp() throws Exception {
        try (IcebergTestFixture fixture = new IcebergTestFixture()) {
            Configuration conf = fixture.configuration("options", 1);
            conf.put(IcebergSourceOptions.PREFIX + "shards-count", "7");
            IcebergTableSourceTest.expectFailure(() -> IcebergTableSourceTest.open(conf));
            for (String key : new String[]{"credential", "header.Authorization", "header.Cookie", "s3.access-key-id"}) {
                Configuration secret = fixture.configuration("secret", 1);
                secret.put(IcebergSourceOptions.PREFIX + "catalog.properties." + key, "sensitive-value");
                try {
                    new IcebergSourceOptions(secret, IcebergTestFixture.OUTPUT.toString());
                    Assert.fail("Inline secret must be rejected");
                } catch (IllegalArgumentException expected) {
                    Assert.assertFalse(expected.getMessage().contains("sensitive-value"));
                }
            }
            Schema timestamp = new Schema(org.apache.iceberg.types.Types.NestedField.optional(1, "ts",
                org.apache.iceberg.types.Types.TimestampType.withoutZone()));
            IcebergTableSourceTest.expectFailure(() -> IcebergRecordMapper.projection(
                new TableSchema(new TableField("ts", Types.TIMESTAMP)), timestamp));
        }
    }

    private static void append(IcebergTestFixture fixture, Record row) throws Exception {
        PartitionKey partition = new PartitionKey(fixture.table.spec(), fixture.table.schema());
        partition.partition(row);
        DataWriter<Record> writer = Parquet.writeData(fixture.table.io().newOutputFile(fixture.file()))
            .schema(fixture.table.schema()).withSpec(fixture.table.spec()).withPartition(partition)
            .setAll(fixture.table.properties()).createWriterFunc(GenericParquetWriter::create).build();
        try (DataWriter<Record> closeable = writer) {
            closeable.write(row);
        }
        fixture.table.newAppend().appendFile(writer.toDataFile()).commit();
    }
}
