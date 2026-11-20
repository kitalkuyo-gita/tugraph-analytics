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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.window.AllFetchWindow;

/** Child JVM: only durable configuration, plan registry and cursor survive. */
public final class IcebergRecoveryProbe {
    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        Map<String, String> values = ScanPlan.JSON.readValue(Files.readString(Path.of(args[0])), Map.class);
        Configuration configuration = new Configuration(values);
        configuration.put(IcebergSourceOptions.PREFIX + "resume", "true");
        IcebergOffset offset = IcebergOffset.decode(Files.readAllBytes(Path.of(args[1])));
        IcebergTableSource source = new IcebergTableSource();
        source.init(configuration, IcebergTestFixture.OUTPUT);
        source.open(null);
        try {
            FetchData<Row> batch = source.fetch(source.listPartitions().get(0), Optional.of(offset), new AllFetchWindow(2));
            java.util.Iterator<Row> rows = batch.getDataIterator();
            int count = 0;
            while (rows.hasNext()) {
                long id = (Long) rows.next().getField(1, Types.LONG);
                if (id != 7 + count++) {
                    throw new AssertionError("Restored row mismatch");
                }
            }
            if (count != 13 || !batch.isFinish()) {
                throw new AssertionError("Restored boundary changed");
            }
        } finally {
            source.close();
        }
    }
}
