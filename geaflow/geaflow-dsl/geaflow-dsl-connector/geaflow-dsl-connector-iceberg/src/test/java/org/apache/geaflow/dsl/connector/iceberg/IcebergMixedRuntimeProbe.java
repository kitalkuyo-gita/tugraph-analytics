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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.TableReadableConnector;
import org.apache.geaflow.dsl.connector.api.TableSource;
import org.apache.geaflow.dsl.connector.api.util.ConnectorFactory;
import org.apache.geaflow.dsl.connector.api.window.AllFetchWindow;

/** Forked host: old file/Paimon dependency tree plus the isolated plugin; no unshaded Iceberg. */
public final class IcebergMixedRuntimeProbe {
    public static void main(String[] args) throws Exception {
        for (String type : new String[]{"file", "paimon", "ICEBERG", "iceberg"}) {
            if (!ConnectorFactory.loadConnector(type).getType().equalsIgnoreCase(type)) {
                throw new AssertionError("SPI mismatch: " + type);
            }
        }
        try {
            Class.forName("org.apache.iceberg.Table");
            throw new AssertionError("Unshaded Iceberg leaked into host classpath");
        } catch (ClassNotFoundException expected) {
            // The host deliberately contains only the relocated Iceberg runtime.
        }
        String parquet = org.apache.parquet.Version.class.getProtectionDomain().getCodeSource().getLocation().toString();
        if (!parquet.contains("1.10.1")) {
            throw new AssertionError("Host must retain file connector's Parquet 1.10.1: " + parquet);
        }
        long physicalRows = org.apache.parquet.hadoop.ParquetFileReader.readFooter(
            new org.apache.hadoop.conf.Configuration(), new org.apache.hadoop.fs.Path(args[1]))
            .getBlocks().stream().mapToLong(org.apache.parquet.hadoop.metadata.BlockMetaData::getRowCount).sum();
        if (physicalRows != 12) {
            throw new AssertionError("Host Parquet reader failed");
        }
        Properties properties = new Properties();
        try (java.io.InputStream input = Files.newInputStream(Path.of(args[0]))) {
            properties.load(input);
        }
        HashMap<String, String> values = new HashMap<>();
        properties.forEach((key, value) -> values.put(key.toString(), value.toString()));
        Configuration conf = new Configuration(values);
        TableSource source = ((TableReadableConnector) ConnectorFactory.loadConnector("iceberg")).createSource(conf);
        source.init(conf, new TableSchema(new TableField("value", Types.STRING), new TableField("id", Types.LONG)));
        source.open(null);
        try {
            List<Long> ids = new ArrayList<>();
            for (Partition partition : source.listPartitions()) {
                FetchData<Row> data = source.fetch(partition, Optional.empty(), new AllFetchWindow(1));
                data.getDataIterator().forEachRemaining(row -> ids.add((Long) row.getField(1, Types.LONG)));
            }
            Collections.sort(ids);
            if (!ids.equals(java.util.Arrays.asList(0L, 1L, 3L, 4L, 5L, 6L, 8L, 9L, 10L, 11L))) {
                throw new AssertionError("Isolated Iceberg rows mismatch: " + ids);
            }
        } finally {
            source.close();
        }
    }
}
