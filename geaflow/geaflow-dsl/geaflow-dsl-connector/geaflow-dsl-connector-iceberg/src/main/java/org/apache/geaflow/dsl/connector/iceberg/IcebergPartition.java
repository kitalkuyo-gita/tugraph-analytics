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

import java.util.Objects;
import org.apache.geaflow.dsl.connector.api.Partition;

/** Scheduling identity is independent of worker index and parallelism. */
public final class IcebergPartition implements Partition {
    private static final long serialVersionUID = 1L;

    private final String runKey;
    private final int shard;

    IcebergPartition(String runKey, int shard) {
        this.runKey = runKey;
        this.shard = shard;
    }

    int shard() {
        return shard;
    }

    @Override
    public String getName() {
        return "iceberg:" + runKey + ":" + shard;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof IcebergPartition)) {
            return false;
        }
        IcebergPartition that = (IcebergPartition) other;
        return shard == that.shard && runKey.equals(that.runKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(runKey, shard);
    }
}
