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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/** Versioned, SDK-object-free recovery envelope. Only the root travels in the execution graph. */
final class ScanPlan {
    static final int VERSION = 1;
    static final String SDK = "1.12.0";
    static final ObjectMapper JSON = new ObjectMapper();

    static final class Root implements Serializable {
        private static final long serialVersionUID = 1L;
        public int version = VERSION;
        public String sdk = SDK;
        public String runKey;
        public String fingerprint;
        public String tableUuid;
        public Long snapshotId;
        public String tableSchema;
        public String projection;
        public String nameMapping;
        public int shards;
        public int[] taskCounts;
        public String[] shardChecksums;
        public String checksum;

        String content() {
            return version + "\n" + sdk + "\n" + runKey + "\n" + fingerprint + "\n"
                + tableUuid + "\n" + snapshotId + "\n" + tableSchema + "\n" + projection
                + "\n" + nameMapping + "\n" + shards + "\n"
                + java.util.Arrays.toString(taskCounts) + "\n" + java.util.Arrays.toString(shardChecksums);
        }

        void validate(IcebergSourceOptions options) {
            if (version != VERSION || !SDK.equals(sdk) || !options.runKey.equals(runKey)
                || !options.fingerprint.equals(fingerprint) || options.shards != shards
                || !hash(content()).equals(checksum) || taskCounts.length != shards
                || shardChecksums.length != shards) {
                throw new IllegalStateException("Incompatible or corrupt Iceberg scan root");
            }
        }
    }

    static final class Task {
        public String key;
        public String json;
        public String checksum;

        Task() {
        }

        Task(String key, String json) {
            this.key = key;
            this.json = json;
            this.checksum = hash(key + "\n" + json);
        }

        void validate() {
            if (!hash(key + "\n" + json).equals(checksum)) {
                throw new IllegalStateException("Corrupt Iceberg task payload");
            }
        }
    }

    final Root root;
    final List<List<Task>> tasks;

    ScanPlan(Root root) {
        this.root = root;
        this.tasks = new ArrayList<>();
        for (int i = 0; i < root.shards; i++) {
            tasks.add(new ArrayList<>());
        }
    }

    void seal() {
        root.taskCounts = new int[root.shards];
        root.shardChecksums = new String[root.shards];
        for (int i = 0; i < root.shards; i++) {
            List<Task> shard = tasks.get(i);
            shard.sort(java.util.Comparator.comparing(task -> task.key));
            StringBuilder digests = new StringBuilder();
            String last = null;
            for (Task task : shard) {
                if (task.key.equals(last)) {
                    throw new IllegalStateException("Duplicate Iceberg scan task");
                }
                last = task.key;
                digests.append(task.checksum).append('\n');
            }
            root.taskCounts[i] = shard.size();
            root.shardChecksums[i] = hash(digests.toString());
        }
        root.checksum = hash(root.content());
    }

    static String encode(Object value) throws IOException {
        return JSON.writeValueAsString(value);
    }

    static String hash(String text) {
        try {
            byte[] bytes = MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8));
            return java.util.HexFormat.of().formatHex(bytes);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
