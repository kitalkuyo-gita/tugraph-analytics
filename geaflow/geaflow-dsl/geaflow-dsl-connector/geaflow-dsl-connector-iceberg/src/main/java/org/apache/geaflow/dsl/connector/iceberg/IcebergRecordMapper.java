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

import java.sql.Date;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.common.binary.BinaryString;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types.NestedField;

final class IcebergRecordMapper {
    private final TableSchema output;
    private final Schema projection;

    IcebergRecordMapper(TableSchema output, Schema projection) {
        this.output = output;
        this.projection = projection;
        if (output.size() != projection.columns().size()) {
            throw new IllegalArgumentException("Iceberg projection changed");
        }
        for (int i = 0; i < output.size(); i++) {
            validateType(output.getField(i), projection.columns().get(i));
        }
    }

    static Schema projection(TableSchema output, Schema schema) {
        if (output.getPartitionSchema().size() != 0) {
            throw new IllegalArgumentException("Declare Iceberg columns as data columns, not DSL partition columns");
        }
        List<NestedField> fields = new ArrayList<>();
        for (TableField field : output.getFields()) {
            NestedField source = schema.findField(field.getName());
            if (source == null || !schema.columns().contains(source)) {
                throw new IllegalArgumentException("Missing top-level Iceberg column: " + field.getName());
            }
            validateType(field, source);
            fields.add(source);
        }
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("At least one Iceberg output column is required");
        }
        return new Schema(fields);
    }

    private static void validateType(TableField target, NestedField source) {
        String type = target.getType().getName();
        String expected;
        switch (source.type().typeId()) {
            case BOOLEAN:
                expected = "BOOLEAN";
                break;
            case INTEGER:
                expected = "INTEGER";
                break;
            case LONG:
                expected = "LONG";
                break;
            case FLOAT:
                expected = "FLOAT";
                break;
            case DOUBLE:
                expected = "DOUBLE";
                break;
            case DECIMAL:
                expected = "DECIMAL";
                break;
            case DATE:
                expected = "DATE";
                break;
            case STRING:
                expected = type.equals("BINARY_STRING") ? "BINARY_STRING" : "STRING";
                break;
            default:
                throw new IllegalArgumentException("Unsupported Iceberg output type: " + source.type());
        }
        if (!type.equals(expected) || (!target.isNullable() && source.isOptional())) {
            throw new IllegalArgumentException("Incompatible Iceberg column: " + target.getName());
        }
    }

    Row map(Record record) {
        Object[] values = new Object[output.size()];
        for (int i = 0; i < values.length; i++) {
            // The reader's augmented schema may include delete columns. Resolve by frozen field ID.
            int id = projection.columns().get(i).fieldId();
            int position = -1;
            for (int j = 0; j < record.struct().fields().size(); j++) {
                if (record.struct().fields().get(j).fieldId() == id) {
                    position = j;
                    break;
                }
            }
            if (position < 0) {
                throw new IllegalStateException("Reader omitted projected Iceberg field " + id);
            }
            Object value = record.get(position);
            if (value != null) {
                switch (output.getField(i).getType().getName()) {
                    case "STRING":
                        value = value.toString();
                        break;
                    case "BINARY_STRING":
                        value = BinaryString.fromString(value.toString());
                        break;
                    case "DATE":
                        value = Date.valueOf((LocalDate) value);
                        break;
                    default: break;
                }
            } else if (!output.getField(i).isNullable()) {
                throw new IllegalStateException("Unexpected null Iceberg column");
            }
            values[i] = value;
        }
        return ObjectRow.create(values);
    }

    long estimatedBytes(Record record) {
        long bytes = 64;
        for (int i = 0; i < record.size(); i++) {
            Object value = record.get(i);
            bytes = Math.addExact(bytes, value instanceof CharSequence
                ? 64L + 2L * ((CharSequence) value).length() : 64L);
        }
        return bytes;
    }
}
