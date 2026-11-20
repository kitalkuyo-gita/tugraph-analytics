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

package org.apache.geaflow.dsl.connector.api.function;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.dsl.connector.api.Offset;
import org.apache.geaflow.store.api.key.IKVStore;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class OffsetStoreRecoveryTest {
    @Test
    @SuppressWarnings("unchecked")
    public void testFailedWriteEvictsCacheAndReconcilesStore() throws Exception {
        OffsetStore store = Mockito.mock(OffsetStore.class, Mockito.CALLS_REAL_METHODS);
        IKVStore<String, Offset> durable = Mockito.mock(IKVStore.class);
        IKVStore<String, String> console = Mockito.mock(IKVStore.class);
        Map<String, Offset> cache = new HashMap<>();
        set(store, "jobId", "test");
        set(store, "tableName", "input");
        set(store, "bucketNum", 10L);
        set(store, "kvStore", durable);
        set(store, "jsonOffsetStore", console);
        set(store, "kvStoreCache", cache);
        Offset previous = Mockito.mock(Offset.class);
        Offset next = Mockito.mock(Offset.class);
        String key = OffsetStore.generateKey("test", "checkpoint_offset", "input", "shard", "2");
        cache.put(key, previous);
        Mockito.doThrow(new IllegalStateException("injected write failure")).when(durable).put(key, next);
        try {
            store.writeOffset("shard", 2, next);
            Assert.fail("Write must fail");
        } catch (RuntimeException expected) {
            Assert.assertFalse(cache.containsKey(key), "Neither old nor uncommitted offset may remain cached");
        }
        Mockito.when(durable.get(key)).thenReturn(previous);
        Assert.assertSame(store.readOffset("shard", 2), previous);
        Mockito.verify(durable).get(key);
        // A console update can fail after the durable cursor committed. Reconcile that case too.
        Mockito.doNothing().when(durable).put(key, next);
        Mockito.doThrow(new IllegalStateException("injected console failure")).when(console)
            .put(Mockito.anyString(), Mockito.anyString());
        try {
            store.writeOffset("shard", 2, next);
            Assert.fail("Console write must fail");
        } catch (RuntimeException expected) {
            Assert.assertFalse(cache.containsKey(key));
        }
        Mockito.when(durable.get(key)).thenReturn(next);
        Assert.assertSame(store.readOffset("shard", 2), next);
    }

    private static void set(Object target, String name, Object value) throws Exception {
        Field field = OffsetStore.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
