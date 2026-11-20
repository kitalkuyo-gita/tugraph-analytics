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
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types.NestedField;

final class IcebergTestFixture implements AutoCloseable {
    static final Schema SCHEMA = new Schema(
        NestedField.optional(1, "id", org.apache.iceberg.types.Types.LongType.get()),
        NestedField.optional(2, "value", org.apache.iceberg.types.Types.StringType.get()));
    static final TableSchema OUTPUT = new TableSchema(new TableField("value", Types.STRING), new TableField("id", Types.LONG));
    final Path directory;
    final HadoopCatalog catalog;
    final Table table;
    final String jdbc;

    IcebergTestFixture() throws Exception {
        this(2);
    }

    IcebergTestFixture(int formatVersion) throws Exception {
        directory = Files.createTempDirectory("geaflow-iceberg-");
        catalog = new HadoopCatalog(new org.apache.hadoop.conf.Configuration(), directory.resolve("warehouse").toUri().toString());
        Map<String, String> properties = new HashMap<>();
        properties.put("format-version", Integer.toString(formatVersion));
        properties.put("write.parquet.compression-codec", "uncompressed");
        properties.put("write.parquet.row-group-size-bytes", "16384");
        properties.put("write.parquet.row-group-check-min-record-count", "10");
        table = catalog.createTable(TableIdentifier.of("db", "records"), SCHEMA, PartitionSpec.unpartitioned(), properties);
        jdbc = "jdbc:h2:file:" + directory.resolve("plans");
        try (Connection connection = DriverManager.getConnection(jdbc); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE geaflow_iceberg_runs (run_key VARCHAR(64) PRIMARY KEY, payload TEXT NOT NULL)");
            statement.execute("CREATE TABLE geaflow_iceberg_tasks (run_key VARCHAR(64) NOT NULL, shard_id INTEGER NOT NULL, "
                + "task_index INTEGER NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(run_key, shard_id, task_index), "
                + "FOREIGN KEY(run_key) REFERENCES geaflow_iceberg_runs(run_key))");
        }
    }

    Configuration configuration(String run, int shards) {
        Configuration conf = new Configuration();
        conf.put(IcebergSourceOptions.PREFIX + "run-id", run);
        conf.put(IcebergSourceOptions.PREFIX + "table", "db.records");
        conf.put(IcebergSourceOptions.PREFIX + "catalog.type", "hadoop");
        conf.put(IcebergSourceOptions.PREFIX + "catalog.properties.warehouse", directory.resolve("warehouse").toUri().toString());
        conf.put(IcebergSourceOptions.PREFIX + "plan.jdbc.url", jdbc);
        conf.put(IcebergSourceOptions.PREFIX + "shard-count", Integer.toString(shards));
        conf.put(IcebergSourceOptions.PREFIX + "split.target-bytes", "16384");
        return conf;
    }

    DataFile append(long start, int count) throws IOException {
        DataWriter<Record> writer = Parquet.writeData(table.io().newOutputFile(file()))
            .schema(table.schema()).withSpec(table.spec()).setAll(table.properties())
            .createWriterFunc(GenericParquetWriter::create).build();
        try (DataWriter<Record> closeable = writer) {
            for (long id = start; id < start + count; id++) {
                GenericRecord record = GenericRecord.create(table.schema());
                record.setField("id", id);
                record.setField("value", "值-" + id);
                closeable.write(record);
            }
        }
        DataFile result = writer.toDataFile();
        table.newAppend().appendFile(result).commit();
        return result;
    }

    void equalityDelete(Long id) throws IOException {
        Schema keys = table.schema().select("id");
        EqualityDeleteWriter<Record> writer = Parquet.writeDeletes(table.io().newOutputFile(file()))
            .rowSchema(keys).withSpec(table.spec()).equalityFieldIds(1).setAll(table.properties())
            .createWriterFunc(GenericParquetWriter::create).buildEqualityWriter();
        try (EqualityDeleteWriter<Record> closeable = writer) {
            GenericRecord record = GenericRecord.create(keys);
            record.setField("id", id);
            closeable.write(record);
        }
        table.newRowDelta().addDeletes(writer.toDeleteFile()).commit();
    }

    void positionDelete(DataFile file, long position) throws IOException {
        PositionDeleteWriter<Record> writer = Parquet.writeDeletes(table.io().newOutputFile(file()))
            .withSpec(table.spec()).setAll(table.properties()).buildPositionWriter();
        try (PositionDeleteWriter<Record> closeable = writer) {
            closeable.write(PositionDelete.<Record>create().set(file.location(), position));
        }
        table.newRowDelta().addDeletes(writer.toDeleteFile()).commit();
    }

    String file() {
        return table.location() + "/data/" + UUID.randomUUID() + ".parquet";
    }

    @Override
    public void close() throws Exception {
        catalog.close();
        try (java.util.stream.Stream<Path> files = Files.walk(directory)) {
            for (Path file : (Iterable<Path>) files.sorted(Comparator.reverseOrder())::iterator) {
                Files.delete(file);
            }
        }
    }
}
