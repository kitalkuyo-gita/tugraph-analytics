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

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.TableReadableConnector;
import org.apache.geaflow.dsl.connector.api.TableSource;
import org.apache.geaflow.dsl.connector.api.window.AllFetchWindow;
import org.testng.Assert;
import org.testng.annotations.Test;

/** Runs after package. The relocated SDK must work across the real GeaFlow API boundary. */
public class IcebergIsolatedIT {
    @Test
    public void testPackagedPluginWithHostDependencies() throws Exception {
        Path jar = Path.of("target", "geaflow-dsl-connector-iceberg-0.8.0-SNAPSHOT-isolated.jar").toAbsolutePath();
        Assert.assertTrue(java.nio.file.Files.isRegularFile(jar), "Build the isolated artifact before integration-test");
        try (IcebergTestFixture fixture = new IcebergTestFixture()) {
            org.apache.iceberg.DataFile file = fixture.append(0, 12);
            fixture.positionDelete(file, 2);
            fixture.equalityDelete(7L);
            long id = 12;
            for (String codec : new String[]{"snappy", "gzip", "zstd"}) {
                fixture.table.updateProperties().set("write.parquet.compression-codec", codec).commit();
                fixture.append(id++, 1);
            }
            ClassLoader parent = Thread.currentThread().getContextClassLoader();
            try (URLClassLoader loader = new URLClassLoader(new URL[]{jar.toUri().toURL()}, parent) {
                @Override
                protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                    if (name.startsWith("org.apache.geaflow.dsl.connector.iceberg.")) {
                        synchronized (getClassLoadingLock(name)) {
                            Class<?> loaded = findLoadedClass(name);
                            if (loaded == null) {
                                loaded = findClass(name);
                            }
                            if (resolve) {
                                resolveClass(loaded);
                            }
                            return loaded;
                        }
                    }
                    return super.loadClass(name, resolve);
                }
            }) {
                Thread.currentThread().setContextClassLoader(loader);
                try {
                    TableReadableConnector connector = (TableReadableConnector) loader
                        .loadClass("org.apache.geaflow.dsl.connector.iceberg.IcebergTableConnector").getDeclaredConstructor().newInstance();
                    Assert.assertSame(connector.getClass().getClassLoader(), loader);
                    TableSource source = connector.createSource(fixture.configuration("isolated", 3));
                    source.init(fixture.configuration("isolated", 3), IcebergTestFixture.OUTPUT);
                    source.open(null);
                    try {
                        List<Long> ids = new ArrayList<>();
                        for (Partition partition : source.listPartitions()) {
                            FetchData<Row> data = source.fetch(partition, Optional.empty(), new AllFetchWindow(1));
                            ids.addAll(IcebergTableSourceTest.rows(data));
                        }
                        Collections.sort(ids);
                        Assert.assertEquals(ids, java.util.Arrays.asList(0L, 1L, 3L, 4L, 5L, 6L, 8L, 9L, 10L, 11L, 12L, 13L, 14L));
                    } finally {
                        source.close();
                    }
                    com.sun.net.httpserver.HttpServer server = IcebergRestCatalogTest.createServer(fixture);
                    try {
                        org.apache.geaflow.common.config.Configuration rest = fixture.configuration("isolated-rest", 3);
                        rest.put(IcebergSourceOptions.PREFIX + "catalog.type", "rest");
                        rest.put(IcebergSourceOptions.PREFIX + "catalog.properties.uri", "http://127.0.0.1:" + server.getAddress().getPort());
                        TableSource restSource = connector.createSource(rest);
                        restSource.init(rest, IcebergTestFixture.OUTPUT);
                        restSource.open(null);
                        try {
                            List<Long> ids = new ArrayList<>();
                            for (Partition partition : restSource.listPartitions()) {
                                FetchData<Row> data = restSource.fetch(partition, Optional.empty(), new AllFetchWindow(1));
                                ids.addAll(IcebergTableSourceTest.rows(data));
                            }
                            Collections.sort(ids);
                            Assert.assertEquals(ids, java.util.Arrays.asList(0L, 1L, 3L, 4L, 5L, 6L, 8L, 9L, 10L, 11L, 12L, 13L, 14L));
                        } finally {
                            restSource.close();
                        }
                    } finally {
                        server.stop(0);
                    }
                } finally {
                    Thread.currentThread().setContextClassLoader(parent);
                }
            }
        }
    }
}
