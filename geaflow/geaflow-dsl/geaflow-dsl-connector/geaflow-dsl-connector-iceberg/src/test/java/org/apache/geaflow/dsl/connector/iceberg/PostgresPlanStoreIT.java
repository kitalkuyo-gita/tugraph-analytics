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
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.geaflow.common.config.Configuration;
import org.testng.Assert;
import org.testng.annotations.Test;

/** Explicit service gate: the postgres profile fails, rather than skips, when its URL is absent. */
public class PostgresPlanStoreIT {
    @Test
    public void testAtomicPublicationConcurrentWinnerAndRecovery() throws Exception {
        String baseUrl = System.getProperty("iceberg.test.postgres.url");
        Assert.assertNotNull(baseUrl, "Set -Diceberg.test.postgres.url when enabling iceberg-postgres-it");
        Class.forName("org.postgresql.Driver");
        Properties auth = new Properties();
        String password = System.getenv("ICEBERG_TEST_PG_PASSWORD");
        if (password != null) {
            auth.setProperty("password", password);
        }
        String schema = "iceberg_test_" + UUID.randomUUID().toString().replace("-", "");
        try (Connection admin = DriverManager.getConnection(baseUrl, auth); Statement ddl = admin.createStatement()) {
            ddl.execute("CREATE SCHEMA " + schema);
            try (IcebergTestFixture fixture = new IcebergTestFixture()) {
                String url = baseUrl + (baseUrl.contains("?") ? "&" : "?") + "currentSchema=" + schema;
                try (Connection connection = DriverManager.getConnection(url, auth); Statement statement = connection.createStatement()) {
                    statement.execute("CREATE TABLE geaflow_iceberg_runs (run_key VARCHAR(64) PRIMARY KEY, payload TEXT NOT NULL)");
                    statement.execute("CREATE TABLE geaflow_iceberg_tasks (run_key VARCHAR(64) REFERENCES geaflow_iceberg_runs(run_key), "
                        + "shard_id INTEGER NOT NULL, task_index INTEGER NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(run_key, shard_id, task_index))");
                }
                fixture.append(0, 5);
                Configuration conf = fixture.configuration("postgres-concurrent", 3);
                conf.put(IcebergSourceOptions.PREFIX + "plan.jdbc.url", url);
                if (password != null) {
                    conf.put(IcebergSourceOptions.PREFIX + "plan.jdbc.password-env", "ICEBERG_TEST_PG_PASSWORD");
                }
                IcebergSourceOptions options = new IcebergSourceOptions(conf, IcebergTestFixture.OUTPUT.toString());
                ScanPlan first;
                ScanPlan second;
                try (IcebergCatalogSession session = new IcebergCatalogSession(options)) {
                    first = IcebergScanPlanner.plan(session, options, IcebergTestFixture.OUTPUT);
                }
                fixture.append(5, 5);
                try (IcebergCatalogSession session = new IcebergCatalogSession(options)) {
                    second = IcebergScanPlanner.plan(session, options, IcebergTestFixture.OUTPUT);
                }
                JdbcScanPlanStore store = new JdbcScanPlanStore(options);
                ExecutorService publishers = Executors.newFixedThreadPool(2);
                try {
                    CountDownLatch start = new CountDownLatch(1);
                    Future<ScanPlan.Root> left = publishers.submit(() -> {
                        start.await();
                        return new JdbcScanPlanStore(options).publish(first);
                    });
                    Future<ScanPlan.Root> right = publishers.submit(() -> {
                        start.await();
                        return new JdbcScanPlanStore(options).publish(second);
                    });
                    start.countDown();
                    Assert.assertEquals(left.get(45, TimeUnit.SECONDS).checksum, right.get(45, TimeUnit.SECONDS).checksum);
                } finally {
                    publishers.shutdownNow();
                }
                ScanPlan.Root winner = store.load();
                int expected = winner.snapshotId.equals(first.root.snapshotId) ? 5 : 10;
                conf.put(IcebergSourceOptions.PREFIX + "resume", "true");
                try (IcebergTableSourceTest.SourceHandle handle = IcebergTableSourceTest.open(conf)) {
                    Assert.assertEquals(IcebergTableSourceTest.read(handle.source, 2).size(), expected);
                }
                // Abort after inserting a root but before inserting any task: no READY root may survive.
                conf.put(IcebergSourceOptions.PREFIX + "run-id", "postgres-abort");
                IcebergSourceOptions aborted = new IcebergSourceOptions(conf, IcebergTestFixture.OUTPUT.toString());
                JdbcScanPlanStore abortedStore = new JdbcScanPlanStore(aborted);
                ScanPlan attempted;
                try (IcebergCatalogSession session = new IcebergCatalogSession(aborted)) {
                    attempted = IcebergScanPlanner.plan(session, aborted, IcebergTestFixture.OUTPUT);
                }
                try (Connection connection = DriverManager.getConnection(url, auth); Statement statement = connection.createStatement()) {
                    statement.execute("ALTER TABLE geaflow_iceberg_tasks ADD CONSTRAINT inject_abort CHECK (task_index < 0) NOT VALID");
                    try {
                        IcebergTableSourceTest.expectFailure(() -> abortedStore.publish(attempted));
                        Assert.assertNull(abortedStore.load());
                    } finally {
                        statement.execute("ALTER TABLE geaflow_iceberg_tasks DROP CONSTRAINT inject_abort");
                    }
                }
                Assert.assertEquals(abortedStore.publish(attempted).checksum, attempted.root.checksum);
            } finally {
                // Only this test's generated namespace is removed.
                ddl.execute("DROP SCHEMA " + schema + " CASCADE");
            }
        }
    }
}
