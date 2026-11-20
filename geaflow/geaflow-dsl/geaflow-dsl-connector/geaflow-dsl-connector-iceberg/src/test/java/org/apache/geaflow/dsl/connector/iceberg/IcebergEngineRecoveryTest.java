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

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.api.window.impl.SizeTumblingWindow;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.function.GeaFlowTableSourceFunction;
import org.apache.geaflow.dsl.connector.api.function.OffsetStore;
import org.apache.geaflow.dsl.connector.api.window.SizeFetchWindow;
import org.apache.geaflow.dsl.schema.GeaFlowTable;
import org.apache.geaflow.store.jdbc.JdbcConfigKeys;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class IcebergEngineRecoveryTest {
    @Test
    public void testRealSourceFunctionCollectFailureJdbcRecoveryAndBucketReuse() throws Exception {
        try (IcebergTestFixture fixture = new IcebergTestFixture()) {
            fixture.append(0, 9);
            Configuration conf = fixture.configuration("engine", 1);
            String jdbc = fixture.jdbc + ";MODE=MySQL;NON_KEYWORDS=VALUE";
            try (Connection connection = DriverManager.getConnection(jdbc); Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE offsets (pk VARCHAR(1024) PRIMARY KEY, value BLOB)");
            }
            conf.put(FrameworkConfigKeys.SYSTEM_OFFSET_BACKEND_TYPE, "JDBC");
            conf.put(ExecutionConfigKeys.JOB_APP_NAME, "iceberg-engine-test");
            conf.put(ExecutionConfigKeys.SYSTEM_META_TABLE, "offsets");
            conf.put(FrameworkConfigKeys.BATCH_NUMBER_PER_CHECKPOINT, "5");
            conf.put(FrameworkConfigKeys.STREAMING_FLYING_BATCH_NUM, "5");
            conf.put(JdbcConfigKeys.JDBC_URL, jdbc);
            conf.put(JdbcConfigKeys.JDBC_DRIVER_CLASS, "org.h2.Driver");
            conf.put(JdbcConfigKeys.JDBC_CONNECTION_POOL_SIZE, "1");
            conf.put(JdbcConfigKeys.JDBC_INSERT_FORMAT, "INSERT INTO %s(%s,%s) VALUES (?, %s)");
            conf.put(JdbcConfigKeys.JDBC_UPDATE_FORMAT, "UPDATE %s SET %s=? WHERE %s='%s'");
            RuntimeContext context = Mockito.mock(RuntimeContext.class);
            Mockito.when(context.getConfiguration()).thenReturn(conf);
            IcebergTableSource source = new IcebergTableSource();
            source.init(conf, IcebergTestFixture.OUTPUT);
            source.open(null);
            Partition partition = source.listPartitions().get(0);
            GeaFlowTable table = Mockito.mock(GeaFlowTable.class);
            Mockito.when(table.getName()).thenReturn("input");
            GeaFlowTableSourceFunction function = new GeaFlowTableSourceFunction(table, source);
            OffsetStore store = new OffsetStore(context, "input");
            set(function, "offsetStore", store);
            set(function, "partitions", source.listPartitions());
            SizeTumblingWindow<Row> window = SizeTumblingWindow.of(4);
            window.initWindow(1);
            List<Long> partial = new ArrayList<>();
            try {
                function.fetch(window, row -> {
                    partial.add((Long) row.getField(1, Types.LONG));
                    if (partial.size() == 2) {
                        throw new IllegalStateException("injected collect failure");
                    }
                    return true;
                });
                Assert.fail("Collect must fail");
            } catch (IllegalStateException expected) {
                Assert.assertNull(store.readOffset(partition.getName(), 2));
            }
            List<Long> replay = new ArrayList<>();
            Assert.assertTrue(function.fetch(window, row -> replay.add((Long) row.getField(1, Types.LONG))));
            Assert.assertEquals(replay, java.util.Arrays.asList(0L, 1L, 2L, 3L));
            IcebergOffset saved = (IcebergOffset) store.readOffset(partition.getName(), 2);
            function.close();
            // New store and source instances load durable state, without the first instance's cache.
            try (OffsetStore restored = new OffsetStore(context, "input")) {
                Assert.assertEquals(((IcebergOffset) restored.readOffset(partition.getName(), 2)).encode(), saved.encode());
                source = new IcebergTableSource();
                source.init(conf, IcebergTestFixture.OUTPUT);
                source.open(null);
                try {
                    Assert.assertEquals(IcebergTableSourceTest.rows(source.fetch(partition, Optional.of(saved), new SizeFetchWindow(2, 4))),
                        java.util.Arrays.asList(4L, 5L, 6L, 7L));
                    IcebergOffset future = new IcebergOffset(saved.planChecksum, saved.shard, saved.taskIndex,
                        saved.taskKey, saved.rowOrdinal, saved.returnedRows, 12);
                    restored.writeOffset(partition.getName(), 12, future);
                    IcebergTableSource active = source;
                    IcebergTableSourceTest.expectFailure(() -> active.fetch(partition,
                        Optional.of(restored.readOffset(partition.getName(), 2)), new SizeFetchWindow(2, 4)));
                } finally {
                    source.close();
                }
            }
        }
    }

    private static void set(Object target, String name, Object value) throws Exception {
        Field field = GeaFlowTableSourceFunction.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
