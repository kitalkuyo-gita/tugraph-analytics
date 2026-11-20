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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/** Atomically publishes a READY root and its tasks in one JDBC transaction. */
final class JdbcScanPlanStore {
    private final IcebergSourceOptions options;

    JdbcScanPlanStore(IcebergSourceOptions options) {
        this.options = options;
    }

    private Connection connect() throws SQLException {
        Properties properties = new Properties();
        for (String key : new String[]{"user", "password"}) {
            String env = options.get("plan.jdbc." + key + "-env", null);
            if (env != null) {
                properties.setProperty(key, IcebergSourceOptions.environment(env));
            }
        }
        return DriverManager.getConnection(options.require("plan.jdbc.url"), properties);
    }

    ScanPlan.Root load() throws IOException {
        try (Connection connection = connect(); PreparedStatement query = connection.prepareStatement(
            "SELECT payload FROM geaflow_iceberg_runs WHERE run_key = ?")) {
            query.setQueryTimeout(30);
            query.setString(1, options.runKey);
            try (ResultSet rows = query.executeQuery()) {
                if (!rows.next()) {
                    return null;
                }
                ScanPlan.Root root = ScanPlan.JSON.readValue(rows.getString(1), ScanPlan.Root.class);
                root.validate(options);
                return root;
            }
        } catch (SQLException e) {
            throw failure(e);
        }
    }

    ScanPlan.Root publish(ScanPlan plan) throws IOException {
        try (Connection connection = connect()) {
            connection.setAutoCommit(false);
            try {
                try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO geaflow_iceberg_runs (run_key, payload) VALUES (?, ?)")) {
                    insert.setQueryTimeout(30);
                    insert.setString(1, options.runKey);
                    insert.setString(2, ScanPlan.encode(plan.root));
                    insert.executeUpdate();
                }
                try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO geaflow_iceberg_tasks (run_key, shard_id, task_index, payload) VALUES (?, ?, ?, ?)")) {
                    insert.setQueryTimeout(30);
                    int pending = 0;
                    for (int shard = 0; shard < plan.root.shards; shard++) {
                        List<ScanPlan.Task> tasks = plan.tasks.get(shard);
                        for (int index = 0; index < tasks.size(); index++) {
                            insert.setString(1, options.runKey);
                            insert.setInt(2, shard);
                            insert.setInt(3, index);
                            insert.setString(4, ScanPlan.encode(tasks.get(index)));
                            insert.addBatch();
                            if (++pending % 256 == 0) {
                                insert.executeBatch();
                            }
                        }
                    }
                    insert.executeBatch();
                }
                connection.commit();
                return plan.root;
            } catch (SQLException | IOException e) {
                try {
                    connection.rollback();
                } catch (SQLException rollback) {
                    e.addSuppressed(rollback);
                }
                // A lost commit response is ambiguous. Reconcile using the immutable run key.
                ScanPlan.Root winner = load();
                if (winner != null) {
                    return winner;
                }
                if (e instanceof SQLException) {
                    throw failure((SQLException) e);
                }
                throw (IOException) e;
            }
        } catch (SQLException e) {
            throw failure(e);
        }
    }

    List<ScanPlan.Task> loadShard(ScanPlan.Root root, int shard) throws IOException {
        List<ScanPlan.Task> tasks = new ArrayList<>();
        StringBuilder digests = new StringBuilder();
        long bytes = 0;
        try (Connection connection = connect(); PreparedStatement query = connection.prepareStatement(
            "SELECT task_index, payload FROM geaflow_iceberg_tasks WHERE run_key = ? AND shard_id = ? ORDER BY task_index")) {
            query.setQueryTimeout(30);
            query.setString(1, root.runKey);
            query.setInt(2, shard);
            query.setFetchSize(256);
            try (ResultSet rows = query.executeQuery()) {
                while (rows.next()) {
                    String payload = rows.getString(2);
                    bytes += payload.length() * 2L;
                    if (rows.getInt(1) != tasks.size() || tasks.size() >= options.maxTasks || bytes > options.maxPlanBytes) {
                        throw new IOException("Invalid or oversized Iceberg shard plan");
                    }
                    ScanPlan.Task task = ScanPlan.JSON.readValue(payload, ScanPlan.Task.class);
                    task.validate();
                    tasks.add(task);
                    digests.append(task.checksum).append('\n');
                }
            }
        } catch (SQLException e) {
            throw failure(e);
        }
        if (tasks.size() != root.taskCounts[shard]
            || !ScanPlan.hash(digests.toString()).equals(root.shardChecksums[shard])) {
            throw new IOException("Iceberg plan missing tasks or checksum mismatch");
        }
        return tasks;
    }

    private static IOException failure(SQLException exception) {
        // JDBC messages can echo URL credentials. Keep SQLState, not the raw server message.
        return new IOException("Iceberg plan store failed; SQLState=" + exception.getSQLState());
    }
}
