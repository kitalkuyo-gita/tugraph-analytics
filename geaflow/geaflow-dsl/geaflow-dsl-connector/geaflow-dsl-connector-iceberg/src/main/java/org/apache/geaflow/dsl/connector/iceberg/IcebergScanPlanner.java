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

import java.io.IOException;
import java.util.List;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ScanTaskParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.CloseableIterable;

final class IcebergScanPlanner {
    static ScanPlan plan(IcebergCatalogSession session, IcebergSourceOptions options, TableSchema output) throws IOException {
        Table table = session.table;
        Snapshot snapshot = resolve(table, options);
        Schema schema = snapshot == null ? table.schema() : table.schemas().get(snapshot.schemaId());
        if (schema == null) {
            throw new IllegalArgumentException("Snapshot schema is no longer available");
        }
        final Schema projection = IcebergRecordMapper.projection(output, schema);
        ScanPlan.Root root = new ScanPlan.Root();
        root.runKey = options.runKey;
        root.fingerprint = options.fingerprint;
        root.tableUuid = session.metadata().uuid();
        root.snapshotId = snapshot == null ? null : snapshot.snapshotId();
        root.tableSchema = SchemaParser.toJson(schema);
        root.projection = SchemaParser.toJson(projection);
        root.nameMapping = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
        root.shards = options.shards;
        ScanPlan plan = new ScanPlan(root);
        long bytes = 0;
        int count = 0;
        if (snapshot != null) {
            try (CloseableIterable<FileScanTask> files = table.newScan().useSnapshot(snapshot.snapshotId())
                .project(projection).planFiles()) {
                for (FileScanTask file : files) {
                    validate(file.file());
                    long deleteBytes = 0;
                    for (DeleteFile delete : file.deletes()) {
                        validate(delete);
                        deleteBytes = Math.addExact(deleteBytes, delete.fileSizeInBytes());
                        List<Integer> ids = delete.equalityFieldIds();
                        if (ids != null) {
                            for (int id : ids) {
                                if (schema.findField(id) == null) {
                                    throw new IllegalArgumentException("Historical delete key is absent from snapshot schema: " + id);
                                }
                            }
                        }
                    }
                    if (deleteBytes > options.maxPlanBytes || file.deletes().size() > 10000) {
                        throw new IllegalArgumentException("Delete set exceeds certified reader budget");
                    }
                    for (FileScanTask split : file.split(options.splitBytes)) {
                        String key = ScanPlan.hash(root.tableUuid + "\n" + root.snapshotId + "\n"
                            + split.spec().specId() + "\n" + split.file().location() + "\n"
                            + split.start() + "\n" + split.length() + "\n" + split.file().content());
                        String json = ScanTaskParser.toJson(split);
                        bytes += json.length() * 2L;
                        if (++count > options.maxTasks || bytes > options.maxPlanBytes) {
                            throw new IllegalArgumentException("Iceberg plan exceeds configured planning budget");
                        }
                        int shard = Math.floorMod(key.hashCode(), options.shards);
                        plan.tasks.get(shard).add(new ScanPlan.Task(key, json));
                    }
                }
            }
        }
        plan.seal();
        return plan;
    }

    private static void validate(ContentFile<?> file) {
        if (file.format() != FileFormat.PARQUET || file.keyMetadata() != null) {
            throw new IllegalArgumentException("Only unencrypted Parquet data and delete files are supported");
        }
    }

    private static Snapshot resolve(Table table, IcebergSourceOptions options) {
        String id = options.get("snapshot-id", null);
        String ref = options.get("ref", null);
        String time = options.get("as-of-timestamp-ms", null);
        Snapshot result;
        if (id != null) {
            result = table.snapshot(Long.parseLong(id));
        } else if (ref != null) {
            SnapshotRef reference = table.refs().get(ref);
            result = reference == null ? null : table.snapshot(reference.snapshotId());
        } else if (time != null) {
            result = table.newScan().asOfTime(Long.parseLong(time)).snapshot();
        } else {
            return table.currentSnapshot();
        }
        if (result == null) {
            throw new IllegalArgumentException("Requested Iceberg snapshot is missing or expired");
        }
        return result;
    }
}
