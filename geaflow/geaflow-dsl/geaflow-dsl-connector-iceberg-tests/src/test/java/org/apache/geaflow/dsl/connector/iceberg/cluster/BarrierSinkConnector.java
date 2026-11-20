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

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.connector.api.TableSink;
import org.apache.geaflow.dsl.connector.api.TableWritableConnector;

/** Test fixture: atomic replacement per window/task, with a durable kill barrier. */
public final class BarrierSinkConnector implements TableWritableConnector {
    @Override
    public String getType() {
        return "ICEBERG_TEST_BARRIER";
    }

    @Override
    public TableSink createSink(Configuration conf) {
        return new Sink();
    }

    public static final class Sink implements TableSink {
        private String jdbc;
        private transient RuntimeContext context;
        private transient Connection transaction;
        private transient PreparedStatement insert;
        private transient long window;
        private transient int ordinal;

        @Override
        public void init(Configuration conf, StructType schema) {
            jdbc = conf.getString("test.jdbc.url");
        }

        @Override
        public void open(RuntimeContext context) {
            this.context = context;
        }

        @Override
        public void write(Row row) throws IOException {
            try {
                begin();
                insert.setLong(1, window);
                insert.setInt(2, context.getTaskArgs().getTaskIndex());
                insert.setInt(3, ordinal++);
                insert.setLong(4, (Long) row.getField(0, Types.LONG));
                insert.setString(5, row.getField(1, Types.STRING).toString());
                insert.executeUpdate();
                if (ordinal == 3) {
                    try (Connection control = FixtureJdbc.connect(jdbc);
                         PreparedStatement claim = control.prepareStatement(
                             "UPDATE test_fault SET pid=?, window_id=? WHERE armed=TRUE AND pid IS NULL AND ? >= min_window")) {
                        claim.setLong(1, ProcessHandle.current().pid());
                        claim.setLong(2, window);
                        claim.setLong(3, window);
                        if (claim.executeUpdate() == 1) {
                            System.out.println("ICEBERG_TEST_KILL_BARRIER pid=" + ProcessHandle.current().pid() + " window=" + window);
                            // Parent sends SIGKILL; a deadline prevents an unattended stalled fixture.
                            Thread.sleep(120000);
                            throw new IOException("Expected worker kill did not arrive");
                        }
                    }
                }
            } catch (SQLException | InterruptedException e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                throw new IOException(e);
            }
        }

        private void begin() throws SQLException {
            if (transaction == null) {
                window = context.getWindowId();
                ordinal = 0;
                transaction = FixtureJdbc.connect(jdbc);
                transaction.setAutoCommit(false);
                try (PreparedStatement delete = transaction.prepareStatement(
                    "DELETE FROM test_output WHERE window_id=? AND task_id=?")) {
                    delete.setLong(1, window);
                    delete.setInt(2, context.getTaskArgs().getTaskIndex());
                    delete.executeUpdate();
                }
                insert = transaction.prepareStatement(
                    "INSERT INTO test_output(window_id,task_id,ordinal,id,payload) VALUES (?,?,?,?,?)");
                try (Connection audit = FixtureJdbc.connect(jdbc);
                     PreparedStatement statement = audit.prepareStatement(
                         "INSERT INTO test_attempts(pid,window_id,task_id) VALUES (?,?,?)")) {
                    statement.setLong(1, ProcessHandle.current().pid());
                    statement.setLong(2, window);
                    statement.setInt(3, context.getTaskArgs().getTaskIndex());
                    statement.executeUpdate();
                }
            }
        }

        @Override
        public void finish() throws IOException {
            if (transaction != null) {
                try {
                    transaction.commit();
                } catch (SQLException e) {
                    throw new IOException(e);
                } finally {
                    close();
                }
            }
        }

        @Override
        public void close() {
            if (transaction != null) {
                try {
                    transaction.close();
                } catch (SQLException e) {
                    throw new IllegalStateException(e);
                } finally {
                    transaction = null;
                    insert = null;
                }
            }
        }
    }
}
