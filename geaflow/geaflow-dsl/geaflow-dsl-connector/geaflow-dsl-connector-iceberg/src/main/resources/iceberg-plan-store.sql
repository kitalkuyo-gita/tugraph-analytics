-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements. See the NOTICE file
-- distributed with this work for additional information regarding
-- copyright ownership. The ASF licenses this file to You under
-- the Apache License, Version 2.0 (the "License"); you may not
-- use this file except in compliance with the License. You may
-- obtain a copy of the License at
-- http://www.apache.org/licenses/LICENSE-2.0
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
-- either express or implied. See the License for the specific
-- language governing permissions and limitations under the License.

-- Provision once using a migration/admin account, not the connector account.
-- PostgreSQL and H2. Do not delete a run until its recovery retention has expired.
CREATE TABLE geaflow_iceberg_runs (
    run_key VARCHAR(64) PRIMARY KEY,
    payload TEXT NOT NULL
);
CREATE TABLE geaflow_iceberg_tasks (
    run_key VARCHAR(64) NOT NULL REFERENCES geaflow_iceberg_runs(run_key),
    shard_id INTEGER NOT NULL,
    task_index INTEGER NOT NULL,
    payload TEXT NOT NULL,
    PRIMARY KEY (run_key, shard_id, task_index)
);
