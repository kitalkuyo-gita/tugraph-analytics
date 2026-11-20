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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.serialize.impl.KryoSerializer;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ScanTaskParser;
import org.apache.iceberg.SchemaParser;
import org.testng.Assert;
import org.testng.annotations.Test;

/** P0 gates run against the actual released SDK and engine serializer. */
public class IcebergCompatibilityTest {
    @Test
    public void testRealTasksDeletesAndDurablePlan() throws Exception {
        try (IcebergTestFixture fixture = new IcebergTestFixture()) {
            DataFile data = fixture.append(0, 3000);
            fixture.append(3000, 3);
            fixture.positionDelete(data, 2000);
            fixture.equalityDelete(12L);
            IcebergSourceOptions options = new IcebergSourceOptions(fixture.configuration("p0", 7), IcebergTestFixture.OUTPUT.toString());
            JdbcScanPlanStore store = new JdbcScanPlanStore(options);
            ScanPlan.Root root;
            try (IcebergCatalogSession session = new IcebergCatalogSession(options)) {
                root = store.publish(IcebergScanPlanner.plan(session, options, IcebergTestFixture.OUTPUT));
            }
            fixture.append(9999, 1);
            ScanPlan.Root restored = (ScanPlan.Root) new KryoSerializer().deserialize(new KryoSerializer().serialize(root));
            Assert.assertNotNull(restored);
            restored.validate(options);
            Assert.assertEquals(store.load().checksum, root.checksum);
            List<Long> rows = read(options, store.load());
            Assert.assertEquals(rows.size(), 3001);
            Assert.assertFalse(rows.contains(12L));
            Assert.assertFalse(rows.contains(2000L));
            Assert.assertFalse(rows.contains(9999L));
            Assert.assertEquals(rows.get(0), Long.valueOf(0));
            Assert.assertEquals(rows.get(rows.size() - 1), Long.valueOf(3002));
            int tasks = java.util.Arrays.stream(root.taskCounts).sum();
            Assert.assertTrue(tasks > 2, "Fixture must exercise Parquet splits, including absolute position deletes");
            // A second publisher must adopt the committed plan, even after latest advanced.
            try (IcebergCatalogSession session = new IcebergCatalogSession(options)) {
                Assert.assertEquals(store.publish(IcebergScanPlanner.plan(session, options, IcebergTestFixture.OUTPUT)).checksum, root.checksum);
            }
        }
    }

    @Test
    public void testIndependentProcessRestoresRootAndTaskPosition() throws Exception {
        try (IcebergTestFixture fixture = new IcebergTestFixture()) {
            fixture.append(0, 20);
            Configuration conf = fixture.configuration("process", 1);
            IcebergSourceOptions options = new IcebergSourceOptions(conf, IcebergTestFixture.OUTPUT.toString());
            try (IcebergCatalogSession session = new IcebergCatalogSession(options)) {
                new JdbcScanPlanStore(options).publish(IcebergScanPlanner.plan(session, options, IcebergTestFixture.OUTPUT));
            }
            java.nio.file.Path configFile = fixture.directory.resolve("config.json");
            java.nio.file.Files.writeString(configFile, ScanPlan.encode(conf.getConfigMap()));
            java.nio.file.Path checkpoint = fixture.directory.resolve("offset.bin");
            IcebergTableSource source = new IcebergTableSource();
            source.init(conf, IcebergTestFixture.OUTPUT);
            source.open(null);
            try {
                org.apache.geaflow.dsl.connector.api.FetchData<org.apache.geaflow.dsl.common.data.Row> batch = source.fetch(
                    source.listPartitions().get(0), java.util.Optional.empty(),
                    new org.apache.geaflow.dsl.connector.api.window.SizeFetchWindow(1, 7));
                org.testng.Assert.assertEquals(IcebergTableSourceTest.rows(batch).size(), 7);
                java.nio.file.Files.write(checkpoint, ((IcebergOffset) batch.getNextOffset()).encode());
            } finally {
                source.close();
            }
            fixture.append(100, 5);
            String javaExecutable = java.nio.file.Path.of(System.getProperty("java.home"), "bin", "java").toString();
            Process process = new ProcessBuilder(javaExecutable, "-cp", System.getProperty("java.class.path"),
                IcebergRecoveryProbe.class.getName(), configFile.toString(), checkpoint.toString()).redirectErrorStream(true)
                .redirectOutput(fixture.directory.resolve("child.log").toFile()).start();
            if (!process.waitFor(60, java.util.concurrent.TimeUnit.SECONDS)) {
                process.destroyForcibly();
                Assert.fail("Recovery process timed out");
            }
            Assert.assertEquals(process.exitValue(), 0, java.nio.file.Files.readString(fixture.directory.resolve("child.log")));
        }
    }

    static List<Long> read(IcebergSourceOptions options, ScanPlan.Root root) throws Exception {
        List<Long> rows = new ArrayList<>();
        JdbcScanPlanStore store = new JdbcScanPlanStore(options);
        try (IcebergCatalogSession session = new IcebergCatalogSession(options)) {
            for (int shard = 0; shard < root.shards; shard++) {
                for (ScanPlan.Task task : store.loadShard(root, shard)) {
                    try (IcebergTaskReader reader = new IcebergTaskReader(session.table.io(), ScanTaskParser.fromJson(task.json, true),
                        SchemaParser.fromJson(root.tableSchema), SchemaParser.fromJson(root.projection), root.nameMapping)) {
                        while (reader.hasNext()) {
                            rows.add((Long) reader.next().getField("id"));
                        }
                    }
                }
            }
        }
        Collections.sort(rows);
        return rows;
    }
}
