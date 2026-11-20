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

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.common.config.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.testng.Assert;
import org.testng.annotations.Test;

/** Exercises the SDK's real REST client and metadata parser against a minimal read-only HTTP catalog. */
public class IcebergRestCatalogTest {
    @Test
    public void testRestCatalogFixedSnapshot() throws Exception {
        try (IcebergTestFixture fixture = new IcebergTestFixture()) {
            fixture.append(0, 5);
            fixture.equalityDelete(2L);
            HttpServer server = createServer(fixture);
            try {
                Configuration conf = fixture.configuration("rest", 3);
                conf.put(IcebergSourceOptions.PREFIX + "catalog.type", "rest");
                conf.put(IcebergSourceOptions.PREFIX + "catalog.properties.uri", "http://127.0.0.1:" + server.getAddress().getPort());
                try (IcebergTableSourceTest.SourceHandle handle = IcebergTableSourceTest.open(conf)) {
                    Assert.assertEquals(IcebergTableSourceTest.read(handle.source, 2), java.util.Arrays.asList(0L, 1L, 3L, 4L));
                }
            } finally {
                server.stop(0);
            }
        }
    }
    static HttpServer createServer(IcebergTestFixture fixture) throws Exception {
        TableMetadata metadata = ((BaseTable) fixture.table).operations().current();
        Map<String, Object> response = new HashMap<>();
        response.put("metadata-location", metadata.metadataFileLocation());
        response.put("metadata", ScanPlan.JSON.readTree(TableMetadataParser.toJson(metadata)));
        final byte[] tableJson = ScanPlan.encode(response).getBytes(StandardCharsets.UTF_8);
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            byte[] body;
            if (path.equals("/v1/config")) {
                body = "{\"defaults\":{},\"overrides\":{}}".getBytes(StandardCharsets.UTF_8);
            } else if (path.equals("/v1/namespaces/db/tables/records")) {
                body = tableJson;
            } else if (path.endsWith("/metrics")) {
                exchange.sendResponseHeaders(204, -1);
                exchange.close();
                return;
            } else {
                exchange.sendResponseHeaders(404, -1);
                exchange.close();
                return;
            }
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (java.io.OutputStream output = exchange.getResponseBody()) {
                output.write(body);
            }
        });
        server.start();
        return server;
    }

}
