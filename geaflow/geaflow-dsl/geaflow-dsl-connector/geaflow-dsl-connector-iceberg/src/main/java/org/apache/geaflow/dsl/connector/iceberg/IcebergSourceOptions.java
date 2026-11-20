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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.geaflow.common.config.Configuration;

/** Validated options. Secret values are resolved on open, never serialized. */
final class IcebergSourceOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    static final String PREFIX = "geaflow.dsl.iceberg.";
    final Map<String, String> values;
    final String runKey;
    final String fingerprint;
    final int shards;
    final int maxTasks;
    final long maxPlanBytes;
    final long maxBatchBytes;
    final long splitBytes;

    IcebergSourceOptions(Configuration conf, String schemaSignature) {
        values = new TreeMap<>();
        conf.getConfigMap().forEach((key, value) -> {
            if (key.startsWith(PREFIX)) {
                values.put(key.substring(PREFIX.length()), value);
            }
        });
        java.util.Set<String> known = new java.util.HashSet<>(java.util.Arrays.asList(
            "run-id", "table", "mode", "catalog.type", "plan.jdbc.url", "plan.jdbc.user-env",
            "plan.jdbc.password-env", "snapshot-id", "as-of-timestamp-ms", "ref", "shard-count",
            "plan.max-tasks", "plan.max-bytes", "batch.max-bytes", "split.target-bytes", "resume"));
        for (String key : values.keySet()) {
            if (!known.contains(key) && !key.startsWith("catalog.properties.") && !key.startsWith("catalog.secret.")) {
                throw new IllegalArgumentException("Unknown Iceberg option: " + key);
            }
        }
        require("run-id");
        require("table");
        require("plan.jdbc.url");
        String catalog = get("catalog.type", "hadoop");
        if (!catalog.equals("hadoop") && !catalog.equals("rest")) {
            throw new IllegalArgumentException("Only hadoop and rest catalogs are supported");
        }
        if (!get("mode", "snapshot").equals("snapshot")) {
            throw new IllegalArgumentException("Only snapshot mode is supported");
        }
        int selectors = 0;
        for (String key : new String[]{"snapshot-id", "as-of-timestamp-ms", "ref"}) {
            if (values.containsKey(key)) {
                selectors++;
            }
        }
        if (selectors > 1) {
            throw new IllegalArgumentException("Snapshot selectors are mutually exclusive");
        }
        shards = (int) number("shard-count", 64, 1, 4096);
        maxTasks = (int) number("plan.max-tasks", 100000, 1, 1000000);
        maxPlanBytes = number("plan.max-bytes", 67108864, 1024, 1073741824);
        maxBatchBytes = number("batch.max-bytes", 67108864, 1024, 1073741824);
        splitBytes = number("split.target-bytes", 134217728, 1, Long.MAX_VALUE);
        if (values.containsKey("snapshot-id")) {
            number("snapshot-id", 0, Long.MIN_VALUE, Long.MAX_VALUE);
        }
        if (values.containsKey("as-of-timestamp-ms")) {
            number("as-of-timestamp-ms", 0, 0, Long.MAX_VALUE);
        }
        if (!get("plan.jdbc.url", "").startsWith("jdbc:")) {
            throw new IllegalArgumentException("plan.jdbc.url must be a JDBC URL");
        }
        for (Map.Entry<String, String> entry : values.entrySet()) {
            String key = entry.getKey().toLowerCase(java.util.Locale.ROOT);
            if (!key.startsWith("catalog.secret.") && !key.endsWith("-env")
                && (key.contains("password") || key.contains("credential") || key.contains("token")
                    || key.contains("authorization") || key.contains("access-key") || key.contains("access_key")
                    || key.endsWith(".cookie")
                    || key.contains("secret") || entry.getValue().matches("(?is).*(password=|token=|://[^/]+@).*"))) {
                throw new IllegalArgumentException("Use environment references for credentials: " + entry.getKey());
            }
        }
        // One source per run-id: reusing a token with a different table/schema must fail.
        runKey = ScanPlan.hash(require("run-id"));
        Map<String, String> semantic = new TreeMap<>(values);
        semantic.remove("resume");
        semantic.remove("plan.jdbc.url");
        semantic.remove("plan.jdbc.user-env");
        semantic.remove("plan.jdbc.password-env");
        try {
            fingerprint = ScanPlan.hash(ScanPlan.encode(semantic) + "\n" + schemaSignature);
        } catch (java.io.IOException e) {
            throw new IllegalStateException("Cannot encode Iceberg options", e);
        }
        if (!get("resume", "false").matches("true|false")) {
            throw new IllegalArgumentException("resume must be true or false");
        }
    }

    String get(String key, String fallback) {
        return values.getOrDefault(key, fallback);
    }

    String require(String key) {
        String value = values.get(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Missing Iceberg option: " + PREFIX + key);
        }
        return value;
    }

    private long number(String key, long fallback, long min, long max) {
        long value;
        try {
            value = Long.parseLong(get(key, Long.toString(fallback)));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid numeric Iceberg option: " + key);
        }
        if (value < min || value > max) {
            throw new IllegalArgumentException("Iceberg option out of range: " + key);
        }
        return value;
    }

    static String environment(String name) {
        String value = System.getenv(name);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("Required credential environment variable is not set: " + name);
        }
        return value;
    }

    Map<String, String> catalogProperties() {
        Map<String, String> result = new HashMap<>();
        values.forEach((key, value) -> {
            if (key.startsWith("catalog.properties.")) {
                result.put(key.substring("catalog.properties.".length()), value);
            } else if (key.startsWith("catalog.secret.")) {
                result.put(key.substring("catalog.secret.".length()), environment(value));
            }
        });
        if (result.containsKey("catalog-impl") || result.containsKey("io-impl")) {
            throw new IllegalArgumentException("Custom Catalog/FileIO implementations are not certified");
        }
        result.put("type", get("catalog.type", "hadoop"));
        return result;
    }
}
