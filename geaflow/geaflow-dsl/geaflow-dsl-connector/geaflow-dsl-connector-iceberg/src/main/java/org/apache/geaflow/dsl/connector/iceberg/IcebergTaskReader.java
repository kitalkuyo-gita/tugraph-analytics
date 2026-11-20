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
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericDeleteFilter;
import org.apache.iceberg.data.IdentityPartitionConverters;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.util.PartitionUtil;

/** Public Iceberg 1.12 adapter: row positions are provided by the format reader. */
final class IcebergTaskReader implements AutoCloseable {
    private final CloseableIterable<Record> records;
    private final CloseableIterator<Record> iterator;

    IcebergTaskReader(FileIO io, FileScanTask task, Schema tableSchema, Schema projection,
                      String nameMapping) throws IOException {
        GenericDeleteFilter deletes = new GenericDeleteFilter(io, task, tableSchema, projection);
        Schema required = deletes.requiredSchema();
        ReadBuilder<Record, ?> builder = FormatModelRegistry.readBuilder(task.file().format(), Record.class,
            io.newInputFile(task.file())).project(required).split(task.start(), task.length())
            .caseSensitive(true).filter(task.residual())
            .idToConstant(PartitionUtil.constantsMap(task, IdentityPartitionConverters::convertConstant));
        if (nameMapping != null) {
            builder.withNameMapping(NameMappingParser.fromJson(nameMapping));
        }
        CloseableIterable<Record> raw = builder.build();
        try {
            InternalRecordWrapper wrapper = new InternalRecordWrapper(required.asStruct());
            Evaluator evaluator = new Evaluator(required.asStruct(), task.residual(), true);
            records = CloseableIterable.filter(deletes.filter(raw), record -> evaluator.eval(wrapper.wrap(record)));
            iterator = records.iterator();
        } catch (RuntimeException e) {
            try {
                raw.close();
            } catch (IOException closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
    }

    boolean hasNext() {
        return iterator.hasNext();
    }

    Record next() {
        return iterator.next();
    }

    @Override
    public void close() throws IOException {
        try {
            iterator.close();
        } finally {
            records.close();
        }
    }
}
