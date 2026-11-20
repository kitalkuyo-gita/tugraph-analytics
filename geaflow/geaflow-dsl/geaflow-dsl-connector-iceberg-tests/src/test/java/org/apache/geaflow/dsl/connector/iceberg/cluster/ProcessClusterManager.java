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

package org.apache.geaflow.dsl.connector.iceberg.cluster;

import java.nio.file.Path;
import org.apache.geaflow.cluster.clustermanager.AbstractClusterManager;
import org.apache.geaflow.cluster.clustermanager.ClusterId;
import org.apache.geaflow.cluster.failover.IFailoverStrategy;
import org.apache.geaflow.cluster.runner.failover.ClusterFailoverStrategy;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.env.IEnvironment.EnvType;

/** Replaces only deployment orchestration with local JVMs, not engine recovery. */
public final class ProcessClusterManager extends AbstractClusterManager {
    @Override
    protected IFailoverStrategy buildFailoverStrategy() {
        ClusterFailoverStrategy strategy = new ClusterFailoverStrategy(EnvType.LOCAL);
        strategy.init(clusterContext);
        strategy.setClusterManager(this);
        return strategy;
    }

    @Override
    public ClusterId startMaster() {
        throw new UnsupportedOperationException("Master is launched by the test supervisor");
    }

    @Override
    protected void createNewDriver(int id, int index) {
        launch("driver", id, index, false);
    }

    @Override
    protected void createNewContainer(int id, boolean recovery) {
        launch("container", id, 0, recovery);
    }

    @Override
    public void restartDriver(int id) {
        launch("driver", id, 0, true);
    }

    @Override
    public void restartContainer(int id) {
        launch("container", id, 0, true);
    }

    private void launch(String role, int id, int index, boolean recovery) {
        try {
            Path directory = Path.of(config.getString(ClusterProcesses.DIRECTORY));
            if (recovery) {
                ClusterProcesses.stop(directory.resolve(role + "-" + id + ".pid"));
            }
            Configuration child = new Configuration(config.getConfigMap());
            child.setMasterId(config.getMasterId());
            child.put("iceberg.test.driver.index", Integer.toString(index));
            child.put("geaflow.work.path", directory.resolve("work-" + role + "-" + id).toString());
            ClusterProcesses.start(directory, role, id, recovery, child);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot launch cluster test process", e);
        }
    }
}
