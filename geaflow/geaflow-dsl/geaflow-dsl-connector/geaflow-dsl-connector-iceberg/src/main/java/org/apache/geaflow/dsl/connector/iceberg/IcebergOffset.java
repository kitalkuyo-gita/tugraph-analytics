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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import org.apache.geaflow.dsl.connector.api.Offset;

/** Next visible row in an immutable plan; no Iceberg or runtime objects enter checkpoints. */
public final class IcebergOffset implements Offset {
    private static final long serialVersionUID = 1L;

    private final int version;
    final String planChecksum;
    final int shard;
    final int taskIndex;
    final String taskKey;
    final long rowOrdinal;
    final long returnedRows;
    final long windowId;

    IcebergOffset(String planChecksum, int shard, int taskIndex, String taskKey, long rowOrdinal, long returnedRows, long windowId) {
        this.version = ScanPlan.VERSION;
        this.planChecksum = planChecksum;
        this.shard = shard;
        this.taskIndex = taskIndex;
        this.taskKey = taskKey;
        this.rowOrdinal = rowOrdinal;
        this.returnedRows = returnedRows;
        this.windowId = windowId;
    }

    void validate(ScanPlan.Root root, int expectedShard, List<ScanPlan.Task> tasks, long expectedWindowId) {
        boolean end = taskIndex == tasks.size();
        if (windowId != expectedWindowId || version != ScanPlan.VERSION || !root.checksum.equals(planChecksum) || shard != expectedShard
            || taskIndex < 0 || taskIndex > tasks.size() || rowOrdinal < 0
            || (returnedRows < 0 && !(end && returnedRows == -1))
            || (end && (rowOrdinal != 0 || taskKey != null))
            || (!end && !tasks.get(taskIndex).key.equals(taskKey))) {
            throw new IllegalArgumentException("Iceberg offset does not match the fixed scan plan");
        }
    }

    public byte[] encode() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeInt(version);
            output.writeUTF(planChecksum);
            output.writeInt(shard);
            output.writeInt(taskIndex);
            output.writeUTF(taskKey == null ? "" : taskKey);
            output.writeLong(rowOrdinal);
            output.writeLong(returnedRows);
            output.writeLong(windowId);
        }
        return bytes.toByteArray();
    }

    public static IcebergOffset decode(byte[] bytes) throws IOException {
        if (bytes.length > 1024) {
            throw new IOException("Oversized Iceberg offset");
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes))) {
            if (input.readInt() != ScanPlan.VERSION) {
                throw new IOException("Unsupported Iceberg offset version");
            }
            String plan = input.readUTF();
            int shard = input.readInt();
            int index = input.readInt();
            String key = input.readUTF();
            IcebergOffset offset = new IcebergOffset(plan, shard, index, key.isEmpty() ? null : key,
                input.readLong(), input.readLong(), input.readLong());
            if (input.available() != 0) {
                throw new IOException("Trailing bytes in Iceberg offset");
            }
            return offset;
        }
    }

    @Override
    public String humanReadable() {
        return "iceberg:" + planChecksum + ":shard=" + shard + ":task=" + taskIndex + ":row=" + rowOrdinal;
    }

    @Override
    public long getOffset() {
        // ALL is lazy: the final count is unknown when its immutable END cursor is created.
        return returnedRows;
    }

    @Override
    public boolean isTimestamp() {
        return false;
    }
}
