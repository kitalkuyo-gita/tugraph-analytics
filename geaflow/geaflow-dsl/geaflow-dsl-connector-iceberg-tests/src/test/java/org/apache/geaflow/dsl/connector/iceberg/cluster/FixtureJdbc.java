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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

final class FixtureJdbc {
    static final String PASSWORD_ENV = "ICEBERG_TEST_PG_PASSWORD";

    static Connection connect(String url) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", "postgres");
        String password = System.getenv(PASSWORD_ENV);
        if (password != null) {
            properties.setProperty("password", password);
        }
        return DriverManager.getConnection(url, properties);
    }
}
