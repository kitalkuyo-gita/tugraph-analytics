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

import java.io.Closeable;
import java.io.IOException;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

final class IcebergCatalogSession implements Closeable {
    private final Catalog catalog;
    final Table table;

    IcebergCatalogSession(IcebergSourceOptions options) throws IOException {
        if (!ScanPlan.SDK.equals(org.apache.iceberg.IcebergBuild.version())) {
            throw new IllegalStateException("Unsupported Iceberg SDK; expected " + ScanPlan.SDK);
        }
        catalog = CatalogUtil.buildIcebergCatalog("geaflow-iceberg", options.catalogProperties(),
            new org.apache.hadoop.conf.Configuration());
        try {
            table = catalog.loadTable(TableIdentifier.parse(options.require("table")));
            metadata();
        } catch (RuntimeException e) {
            try {
                close();
            } catch (IOException closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
    }

    TableMetadata metadata() {
        if (!(table instanceof BaseTable)) {
            throw new IllegalArgumentException("Unsupported Iceberg Table implementation");
        }
        TableMetadata metadata = ((BaseTable) table).operations().current();
        if (metadata.formatVersion() < 1 || metadata.formatVersion() > 2) {
            throw new IllegalArgumentException("Only Iceberg format v1/v2 is supported");
        }
        if (metadata.uuid() == null) {
            throw new IllegalArgumentException("Iceberg table must have a stable UUID");
        }
        return metadata;
    }

    @Override
    public void close() throws IOException {
        if (catalog instanceof Closeable) {
            ((Closeable) catalog).close();
        }
    }
}
