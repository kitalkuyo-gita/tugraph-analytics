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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.geaflow.cluster.client.AbstractClusterClient;
import org.apache.geaflow.cluster.client.AbstractEnvironment;
import org.apache.geaflow.cluster.client.IClusterClient;
import org.apache.geaflow.cluster.client.IPipelineClient;
import org.apache.geaflow.cluster.client.PipelineClientFactory;
import org.apache.geaflow.cluster.clustermanager.ClusterInfo;
import org.apache.geaflow.cluster.container.Container;
import org.apache.geaflow.cluster.container.ContainerContext;
import org.apache.geaflow.cluster.driver.Driver;
import org.apache.geaflow.cluster.driver.DriverContext;
import org.apache.geaflow.cluster.rpc.ConnectAddress;
import org.apache.geaflow.cluster.runner.entrypoint.MasterRunner;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.serialize.SerializerFactory;
import org.apache.geaflow.dsl.runtime.engine.GQLPipeLine.GQLPipelineTask;
import org.apache.geaflow.dsl.runtime.util.QueryUtil;
import org.apache.geaflow.pipeline.IPipelineResult;
import org.apache.geaflow.pipeline.Pipeline;
import org.apache.geaflow.pipeline.PipelineFactory;
import org.apache.geaflow.pipeline.callback.ICallbackFunction;

public final class ClusterProcessMain {
    public static void main(String[] args) throws Exception {
        String role = args[0];
        int id = Integer.parseInt(args[1]);
        boolean recovery = Boolean.parseBoolean(args[2]);
        System.out.println((recovery ? "ICEBERG_TEST_RECOVERED_PROCESS" : "ICEBERG_TEST_INITIAL_PROCESS")
            + " role=" + role + " id=" + id + " pid=" + ProcessHandle.current().pid());
        Configuration config = ClusterProcesses.load(Path.of(args[3]));
        String password = System.getenv(FixtureJdbc.PASSWORD_ENV);
        if (password != null) {
            config.put("geaflow.store.jdbc.password", password);
        }
        Path directory = Path.of(config.getString(ClusterProcesses.DIRECTORY));
        if (role.equals("master")) {
            ClusterInfo info = new MasterRunner(config, new ProcessClusterManager()).init();
            Files.write(directory.resolve("cluster-info"), SerializerFactory.getKryoSerializer().serialize(info));
            Files.writeString(directory.resolve(recovery ? "master-recovered" : "master-ready"), "ready");
        } else if (role.equals("driver")) {
            DriverContext context = new DriverContext(id, Integer.parseInt(config.getString("iceberg.test.driver.index")), config, recovery);
            context.load();
            new Driver().init(context);
        } else if (role.equals("container")) {
            ContainerContext context = new ContainerContext(id, config, recovery);
            context.load();
            new Container().init(context);
        } else if (role.equals("client")) {
            runClient(config, directory);
            System.exit(0);
        } else {
            throw new IllegalArgumentException(role);
        }
        new java.util.concurrent.CountDownLatch(1).await();
    }

    private static void runClient(Configuration config, Path directory) throws Exception {
        TestEnvironment environment = new TestEnvironment();
        environment.getEnvironmentContext().withConfig(config.getConfigMap());
        try {
            String sql = Files.readString(directory.resolve("query.sql"));
            QueryUtil.preCompile(sql, config);
            Pipeline pipeline = PipelineFactory.buildPipeline(environment);
            pipeline.submit(new GQLPipelineTask(sql, null, null)).addCallBack(new Completion(directory.toString()));
            IPipelineResult<?> result = pipeline.execute();
            CompletableFuture.runAsync(() -> {
                try {
                    result.get();
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            });
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(240);
            while (!Files.exists(directory.resolve("completed"))) {
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("Cluster SQL did not complete; inspect component logs: " + directory);
                }
                Thread.sleep(100);
            }
        } finally {
            environment.shutdown();
        }
    }

    public static final class Completion implements ICallbackFunction {
        private final String directory;

        public Completion(String directory) {
            this.directory = directory;
        }

        @Override
        public void window(long windowId) {
            record("completed-window", Long.toString(windowId));
        }

        @Override
        public void terminal() {
            record("completed", "success");
        }

        private void record(String file, String value) {
            try {
                Files.writeString(Path.of(directory, file), value);
            } catch (java.io.IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    public static final class TestEnvironment extends AbstractEnvironment {
        @Override
        protected IClusterClient getClusterClient() {
            return new TestClient();
        }

        @Override
        public EnvType getEnvType() {
            return EnvType.LOCAL;
        }
    }

    public static final class TestClient extends AbstractClusterClient {
        private final AtomicBoolean closing = new AtomicBoolean();
        private Path directory;
        private Process master;

        @Override
        public IPipelineClient startCluster() {
            try {
                directory = Path.of(config.getString(ClusterProcesses.DIRECTORY));
                master = ClusterProcesses.start(directory, "master", 0, false, config);
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
                while (!Files.exists(directory.resolve("master-ready"))) {
                    if (!master.isAlive() || System.nanoTime() > deadline) {
                        throw new IllegalStateException("Master did not start: " + directory);
                    }
                    Thread.sleep(100);
                }
                Thread supervisor = new Thread(() -> {
                    try {
                        int exit = master.waitFor();
                        if (!closing.get()) {
                            Files.writeString(directory.resolve("master-exit"), Integer.toString(exit));
                            master = ClusterProcesses.start(directory, "master", 0, true, config);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }, "test-master-supervisor");
                supervisor.setDaemon(true);
                supervisor.start();
                ClusterInfo info = (ClusterInfo) SerializerFactory.getKryoSerializer()
                    .deserialize(Files.readAllBytes(directory.resolve("cluster-info")));
                Map<String, ConnectAddress> drivers = info.getDriverAddresses();
                return PipelineClientFactory.createPipelineClient(drivers, config);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void shutdown() {
            closing.set(true);
            if (directory != null) {
                try {
                    // Client is stopped by its owner; do not kill this JVM from its own PID file.
                    try (java.util.stream.Stream<Path> paths = Files.list(directory)) {
                        for (Path file : (Iterable<Path>) paths.filter(path -> path.toString().endsWith(".pid")
                            && !path.getFileName().toString().startsWith("client-"))::iterator) {
                            ClusterProcesses.stop(file);
                        }
                    }
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        }
    }
}
