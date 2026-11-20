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

package org.apache.geaflow.dsl.connector.iceberg.cluster;

import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.dsl.connector.iceberg.shaded.org.apache.iceberg.DataFile;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class IcebergClusterIT {
    @DataProvider(name = "scenarios")
    public Object[][] scenarios() {
        Object[][] scenarios = {{"all", -1, 1, false}, {"size", 10, 1, false},
            {"size-kill", 10, 1, true}, {"flying-kill", 10, 3, true}, {"graph", -1, 1, false}};
        String selected = System.getProperty("iceberg.test.scenario");
        if (selected == null) {
            return scenarios;
        }
        for (Object[] scenario : scenarios) {
            if (scenario[0].equals(selected)) {
                return new Object[][]{scenario};
            }
        }
        throw new IllegalArgumentException("Unknown cluster test scenario: " + selected);
    }

    @Test(dataProvider = "scenarios", timeOut = 360000)
    public void testSqlOnRealWorkerProcesses(String name, int window, int flying, boolean kill) throws Exception {
        String url = System.getProperty("iceberg.test.postgres.url");
        String redisPort = System.getProperty("iceberg.test.redis.port");
        Assert.assertNotNull(url, "Supply a disposable PostgreSQL URL for cluster IT");
        Assert.assertNotNull(redisPort, "Supply the disposable Redis port for cluster IT");
        Path directory = Files.createTempDirectory(Path.of("target").toAbsolutePath(), "cluster-" + name + "-");
        try (ClusterFixture fixture = new ClusterFixture(directory, url)) {
            Map<String, Integer> expected = new HashMap<>();
            for (int batch = 0; batch < 12; batch++) {
                DataFile file = fixture.append(batch * 40, 40);
                if (batch == 0) {
                    fixture.positionDelete(file, 7);
                }
            }
            fixture.equalityDelete(23L);
            fixture.append(40, 2); // Actual duplicate business rows must not be deduplicated by assertions.
            for (int id = 5; id < 480; id++) {
                if (id != 7 && id != 23) {
                    expected.put(id + "|值-" + id, id == 40 || id == 41 ? 2 : 1);
                }
            }
            provision(fixture.jdbc, kill);
            Configuration source = fixture.configuration("run-" + name, 17);
            StringBuilder options = new StringBuilder("type='iceberg'");
            for (Map.Entry<String, String> entry : source.getConfigMap().entrySet()) {
                options.append(",\n `").append(entry.getKey()).append("`='").append(entry.getValue()).append("'");
            }
            String sql = "CREATE TABLE src(id BIGINT,payload VARCHAR) WITH (" + options + ");\n"
                + "CREATE TABLE dst(id BIGINT,payload VARCHAR) WITH (type='iceberg_test_barrier', `test.jdbc.url`='" + fixture.jdbc + "');\n"
                + "INSERT INTO dst SELECT id,payload FROM src WHERE id>=5" + (window < 0 ? " ORDER BY id" : "") + ";\n";
            if (name.equals("graph")) {
                expected.replaceAll((key, count) -> 1); // Graph vertex identity collapses repeated input IDs.
                sql = sql.substring(0, sql.indexOf("INSERT INTO dst"))
                    + "CREATE GRAPH g(Vertex person(id BIGINT ID,payload VARCHAR),"
                    + "Edge knows(srcId BIGINT SOURCE ID,targetId BIGINT DESTINATION ID)) "
                    + "WITH(storeType='rocksdb',shardCount=2);\n"
                    + "INSERT INTO g.person SELECT id,payload FROM src WHERE id>=5;\n"
                    + "USE GRAPH g;\n"
                    + "INSERT INTO dst SELECT id,payload FROM (MATCH (a:person) RETURN a.id AS id,a.payload AS payload);\n";
            }
            Files.writeString(directory.resolve("query.sql"), sql);
            Configuration config = configure(directory, fixture.jdbc, redisPort, window, flying);
            Process client = ClusterProcesses.start(directory, "client", 0, false, config);
            try {
                String before = null;
                if (kill) {
                    long victim = waitForBarrier(fixture.jdbc, client, directory);
                    before = plan(fixture.jdbc);
                    Files.writeString(directory.resolve("plan-before.json"), before);
                    Assert.assertTrue(scalar(fixture.jdbc, "SELECT COUNT(*) FROM test_output WHERE window_id < 3") > 0,
                        "Kill only after earlier output windows have committed");
                    Assert.assertTrue(scalar(fixture.jdbc, "SELECT COUNT(*) FROM test_offsets") > 0,
                        "Kill only after durable source offsets exist");
                    diagnostics(fixture.jdbc, directory.resolve("before-kill.txt"));
                    try (Connection connection = FixtureJdbc.connect(fixture.jdbc); Statement statement = connection.createStatement()) {
                        statement.executeUpdate("UPDATE test_fault SET armed=FALSE");
                    }
                    boolean ownedWorker;
                    try (java.util.stream.Stream<Path> files = Files.list(directory)) {
                        ownedWorker = files.filter(path -> path.getFileName().toString().startsWith("container-")
                            && path.toString().endsWith(".pid")).anyMatch(path -> {
                                try {
                                    return Long.parseLong(Files.readString(path)) == victim;
                                } catch (Exception e) {
                                    throw new IllegalStateException(e);
                                }
                            });
                    }
                    Assert.assertTrue(ownedWorker, "Kill barrier must belong to a real container process");
                    ProcessHandle handle = ProcessHandle.of(victim).orElseThrow();
                    Assert.assertTrue(handle.destroyForcibly());
                    handle.onExit().get(20, TimeUnit.SECONDS);
                    fixture.append(10000, 5); // Recovered plan must not switch to latest.
                    Files.writeString(directory.resolve("killed-worker"), Long.toString(victim));
                }
                Assert.assertTrue(client.waitFor(300, TimeUnit.SECONDS), "Cluster timed out: " + directory);
                Assert.assertEquals(client.exitValue(), 0, "Inspect cluster logs: " + directory);
                Assert.assertTrue(Files.exists(directory.resolve("completed")));
                Assert.assertEquals(results(fixture.jdbc), expected, "Exact visible-row multiset; logs: " + directory);
                try (java.util.stream.Stream<Path> files = Files.list(directory)) {
                    Assert.assertEquals(files.filter(path -> path.getFileName().toString().startsWith("container-")
                        && path.toString().endsWith(".initial-pid")).count(), 4L);
                }
                int sourceProcesses = 0;
                try (java.util.stream.Stream<Path> files = Files.list(directory)) {
                    for (Path file : (Iterable<Path>) files.filter(path -> path.getFileName().toString().startsWith("container-")
                        && path.toString().endsWith(".log"))::iterator) {
                        String log = Files.readString(file);
                        // Assignment after failover may use a different free container.
                        String initialLog = log.split("ICEBERG_TEST_RECOVERED_PROCESS", 2)[0];
                        if (initialLog.contains("open source table: src,")) {
                            Assert.assertTrue(initialLog.contains("parallel: 3, assigned"), file.toString());
                            sourceProcesses++;
                        }
                    }
                }
                Assert.assertEquals(sourceProcesses, 3, "Every worker process must execute an Iceberg source task");
                if (kill) {
                    Assert.assertTrue(Files.exists(directory.resolve("master-exit")), "Engine must detect worker loss and trigger failover");
                    Assert.assertTrue(Files.exists(directory.resolve("master-recovered")), "Master must restore its durable state");
                    Assert.assertNotEquals(Files.readString(directory.resolve("master-exit")).trim(), "0");
                    Assert.assertEquals(plan(fixture.jdbc), before, "Recovery must reuse the exact durable plan");
                    Files.writeString(directory.resolve("plan-after.json"), plan(fixture.jdbc));
                    Assert.assertTrue(scalar(fixture.jdbc, "SELECT COUNT(*) FROM (SELECT window_id,task_id "
                        + "FROM test_attempts GROUP BY window_id,task_id HAVING COUNT(DISTINCT pid)>1) replayed") > 0,
                        "A window/task must actually be replayed in a different process");
                    Assert.assertEquals(scalar(fixture.jdbc, "SELECT COUNT(*) FROM test_output WHERE id>=10000"), 0L);
                    try (java.util.stream.Stream<Path> files = Files.list(directory)) {
                        for (Path recovered : (Iterable<Path>) files.filter(path -> path.toString().endsWith(".recovered-pid"))::iterator) {
                            Path initial = Path.of(recovered.toString().replace(".recovered-pid", ".initial-pid"));
                            Assert.assertNotEquals(Files.readString(recovered), Files.readString(initial));
                        }
                    }
                    String driverLogs;
                    try (java.util.stream.Stream<Path> files = Files.list(directory)) {
                        Path driver = files.filter(path -> path.getFileName().toString().startsWith("driver-")
                            && path.toString().endsWith(".log")).findFirst().orElseThrow();
                        driverLogs = Files.readString(driver);
                    }
                    Assert.assertTrue(driverLogs.contains("execute pipeline from recovered context"));
                }
                Files.writeString(directory.resolve("acceptance.txt"), "PASS scenario=" + name
                    + " sourceProcesses=3 containers=4 flying=" + flying + " rows="
                    + expected.values().stream().mapToInt(Integer::intValue).sum() + " kill=" + kill + "\n");
            } finally {
                ClusterProcesses.stopAll(directory);
                diagnostics(fixture.jdbc, directory.resolve("database-evidence.txt"));
            }
        }
    }

    private static long scalar(String jdbc, String sql) throws Exception {
        try (Connection connection = FixtureJdbc.connect(jdbc); Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery(sql)) {
            Assert.assertTrue(rows.next());
            return rows.getLong(1);
        }
    }

    private static void diagnostics(String jdbc, Path file) throws Exception {
        StringBuilder text = new StringBuilder();
        try (Connection connection = FixtureJdbc.connect(jdbc); Statement statement = connection.createStatement()) {
            String[] queries = {"SELECT * FROM test_fault", "SELECT * FROM test_attempts ORDER BY window_id,task_id,pid",
                "SELECT window_id,task_id,COUNT(*) FROM test_output GROUP BY window_id,task_id ORDER BY window_id,task_id",
                "SELECT COUNT(*) AS durable_offset_rows FROM test_offsets"};
            for (String query : queries) {
                text.append(query).append('\n');
                try (ResultSet rows = statement.executeQuery(query)) {
                    while (rows.next()) {
                        for (int column = 1; column <= rows.getMetaData().getColumnCount(); column++) {
                            text.append(rows.getObject(column)).append('\t');
                        }
                        text.append('\n');
                    }
                }
            }
        }
        Files.writeString(file, text);
    }

    private static Configuration configure(Path directory, String jdbc, String redisPort, int window, int flying) throws Exception {
        Configuration conf = new Configuration();
        conf.put(ClusterProcesses.DIRECTORY, directory.toString());
        conf.put("geaflow.job.unique.id", directory.getFileName().toString());
        conf.put("geaflow.job.runtime.name", directory.getFileName().toString());
        conf.put("geaflow.job.cluster.id", directory.getFileName().toString());
        conf.put("geaflow.work.path", directory.resolve("work-client").toString());
        conf.put("geaflow.file.persistent.root", directory.resolve("checkpoints").toString());
        conf.put("geaflow.run.local.mode", "false");
        conf.put("geaflow.ha.service.type", "redis");
        conf.put("geaflow.store.redis.host", "127.0.0.1");
        conf.put("geaflow.store.redis.port", redisPort);
        conf.put("geaflow.container.num", "4");
        conf.put("geaflow.container.worker.num", "1");
        conf.put("geaflow.fo.strategy", "cluster_fo");
        conf.put("geaflow.heartbeat.interval.ms", "1000");
        conf.put("geaflow.heartbeat.initial.delay.ms", "1000");
        conf.put("geaflow.heartbeat.timeout.ms", "10000");
        conf.put("geaflow.system.state.backend.type", "ROCKSDB");
        conf.put("geaflow.system.offset.backend.type", "JDBC");
        conf.put("geaflow.system.meta.table", "test_offsets");
        conf.put("geaflow.store.jdbc.url", jdbc);
        conf.put("geaflow.store.jdbc.user.name", "postgres");
        conf.put("geaflow.store.jdbc.driver.class", "org.postgresql.Driver");
        conf.put("geaflow.store.jdbc.connection.pool.size", "2");
        conf.put("geaflow.store.jdbc.insert.format", "INSERT INTO %s(%s,%s) VALUES (?,%s)");
        conf.put("geaflow.store.jdbc.update.format", "UPDATE %s SET %s=? WHERE %s='%s'");
        conf.put("geaflow.dsl.window.size", Integer.toString(window));
        conf.put("geaflow.dsl.table.parallelism", "3");
        conf.put("geaflow.dsl.source.parallelism", "3");
        conf.put("geaflow.batch.number.per.checkpoint", "1");
        conf.put("geaflow.streaming.flying.batch.num", Integer.toString(flying));
        conf.put("geaflow.http.rest.service.enable", "false");
        try (ServerSocket socket = new ServerSocket(0)) {
            conf.put("geaflow.master.http.port", Integer.toString(socket.getLocalPort()));
        }
        return conf;
    }

    private static void provision(String jdbc, boolean kill) throws Exception {
        try (Connection connection = FixtureJdbc.connect(jdbc); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE test_offsets(pk VARCHAR(1024) PRIMARY KEY,value BYTEA NOT NULL)");
            statement.execute("CREATE TABLE test_output(window_id BIGINT,task_id INT,ordinal INT,id BIGINT,payload TEXT,"
                + "PRIMARY KEY(window_id,task_id,ordinal))");
            statement.execute("CREATE TABLE test_attempts(pid BIGINT,window_id BIGINT,task_id INT)");
            statement.execute("CREATE TABLE test_fault(armed BOOLEAN,pid BIGINT,window_id BIGINT,min_window BIGINT)");
            statement.execute("INSERT INTO test_fault(armed,min_window) VALUES (" + kill + ",3)");
        }
    }

    private static long waitForBarrier(String jdbc, Process client, Path directory) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(180);
        while (System.nanoTime() < deadline && client.isAlive()) {
            try (Connection connection = FixtureJdbc.connect(jdbc); Statement statement = connection.createStatement();
                 ResultSet rows = statement.executeQuery("SELECT pid FROM test_fault WHERE pid IS NOT NULL")) {
                if (rows.next()) {
                    return rows.getLong(1);
                }
            }
            Thread.sleep(100);
        }
        throw new AssertionError("Worker did not reach kill barrier: " + directory);
    }

    private static String plan(String jdbc) throws Exception {
        try (Connection connection = FixtureJdbc.connect(jdbc); Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery("SELECT payload FROM geaflow_iceberg_runs")) {
            Assert.assertTrue(rows.next());
            return rows.getString(1);
        }
    }

    private static Map<String, Integer> results(String jdbc) throws Exception {
        Map<String, Integer> results = new HashMap<>();
        try (Connection connection = FixtureJdbc.connect(jdbc); Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery("SELECT id,payload,COUNT(*) FROM test_output GROUP BY id,payload")) {
            while (rows.next()) {
                results.put(rows.getLong(1) + "|" + rows.getString(2), rows.getInt(3));
            }
        }
        return results;
    }
}
