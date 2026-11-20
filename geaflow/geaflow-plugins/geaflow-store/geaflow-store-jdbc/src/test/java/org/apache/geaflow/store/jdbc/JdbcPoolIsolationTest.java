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

package org.apache.geaflow.store.jdbc;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.state.serializer.DefaultKVSerializer;
import org.apache.geaflow.store.context.StoreContext;
import org.testng.Assert;
import org.testng.annotations.Test;

public class JdbcPoolIsolationTest {
    @Test
    public void testIndependentDatabasesCloseAndReopen() throws Exception {
        Path directory = Files.createTempDirectory("geaflow-jdbc-pools-");
        JdbcKVStore<String, String> first = new JdbcKVStore<>();
        JdbcKVStore<String, String> second = new JdbcKVStore<>();
        try {
            Configuration firstConfig = prepare(directory.resolve("first.db"));
            Configuration secondConfig = prepare(directory.resolve("second.db"));
            first.init(context(firstConfig));
            second.init(context(secondConfig));
            first.put("same-key", "first");
            second.put("same-key", "second");
            Assert.assertEquals(first.get("same-key"), "first");
            Assert.assertEquals(second.get("same-key"), "second");
            first.close();
            first.close();
            Assert.assertEquals(second.get("same-key"), "second");
            first.init(context(firstConfig));
            Assert.assertEquals(first.get("same-key"), "first");
        } finally {
            first.close();
            second.close();
            try (java.util.stream.Stream<Path> files = Files.walk(directory)) {
                for (Path file : (Iterable<Path>) files.sorted(java.util.Comparator.reverseOrder())::iterator) {
                    Files.delete(file);
                }
            }
        }
    }

    private static StoreContext context(Configuration conf) {
        return new StoreContext("offsets").withConfig(conf)
            .withKeySerializer(new DefaultKVSerializer<>(String.class, String.class));
    }

    private static Configuration prepare(Path path) throws Exception {
        String url = "jdbc:sqlite:" + path;
        Class.forName("org.sqlite.JDBC");
        try (Connection connection = DriverManager.getConnection(url); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE offsets (pk VARCHAR(256) PRIMARY KEY, value BLOB)");
        }
        Configuration conf = new Configuration();
        conf.put(JdbcConfigKeys.JDBC_DRIVER_CLASS, "org.sqlite.JDBC");
        conf.put(JdbcConfigKeys.JDBC_URL, url);
        conf.put(JdbcConfigKeys.JDBC_CONNECTION_POOL_SIZE, "1");
        conf.put(JdbcConfigKeys.JDBC_INSERT_FORMAT, "INSERT INTO %s(%s,%s) VALUES (?, %s)");
        conf.put(JdbcConfigKeys.JDBC_UPDATE_FORMAT, "UPDATE %s SET %s=? WHERE %s='%s'");
        return conf;
    }
}
