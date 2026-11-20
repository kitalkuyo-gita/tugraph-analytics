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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.geaflow.common.config.Configuration;

/** Test-only process launcher. Scheduling, RPC and recovery remain engine-owned. */
final class ClusterProcesses {
    static final String DIRECTORY = "iceberg.test.directory";

    static Configuration load(Path file) throws IOException {
        Properties values = new Properties();
        try (java.io.InputStream in = Files.newInputStream(file)) {
            values.load(in);
        }
        Configuration config = new Configuration();
        values.forEach((key, value) -> config.put(key.toString(), value.toString()));
        config.setMasterId(values.getProperty("iceberg.test.master.id", "_MASTER"));
        return config;
    }

    static void save(Path file, Configuration config) throws IOException {
        Properties values = new Properties();
        values.putAll(config.getConfigMap());
        // Children resolve the disposable service credential from their inherited environment.
        values.remove("geaflow.store.jdbc.password");
        if (config.getMasterId() != null) {
            values.setProperty("iceberg.test.master.id", config.getMasterId());
        }
        Path temporary = file.resolveSibling(file.getFileName() + ".tmp");
        try (java.io.OutputStream out = Files.newOutputStream(temporary)) {
            values.store(out, "Disposable cluster test configuration");
        }
        Files.move(temporary, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    static Process start(Path directory, String role, int id, boolean recovery, Configuration config) throws IOException {
        String name = role + "-" + id;
        Path configFile = directory.resolve(name + ".properties");
        save(configFile, config);
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-Xmx512m");
        command.add("-XX:ActiveProcessorCount=2");
        for (String argument : ManagementFactory.getRuntimeMXBean().getInputArguments()) {
            if (argument.startsWith("--add-opens=") || argument.startsWith("--add-exports=")
                || argument.startsWith("-Dio.netty.") || argument.startsWith("-Djdk.reflect.")) {
                command.add(argument);
            }
        }
        command.add("-cp");
        command.add(System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")));
        command.add(ClusterProcessMain.class.getName());
        command.add(role);
        command.add(Integer.toString(id));
        command.add(Boolean.toString(recovery));
        command.add(configFile.toString());
        Process process = new ProcessBuilder(command).redirectErrorStream(true)
            .redirectOutput(ProcessBuilder.Redirect.appendTo(directory.resolve(name + ".log").toFile())).start();
        Files.writeString(directory.resolve(name + ".pid"), Long.toString(process.pid()));
        Files.writeString(directory.resolve(name + (recovery ? ".recovered-pid" : ".initial-pid")), Long.toString(process.pid()));
        return process;
    }

    static void stop(Path pidFile) throws Exception {
        if (Files.exists(pidFile)) {
            long pid = Long.parseLong(Files.readString(pidFile).trim());
            ProcessHandle process = ProcessHandle.of(pid).orElse(null);
            if (process != null && process.isAlive()) {
                process.destroyForcibly();
                process.onExit().get(20, TimeUnit.SECONDS);
            }
        }
    }

    static void stopAll(Path directory) throws Exception {
        // Stop the supervisor first so cleanup cannot initiate another recovery.
        stop(directory.resolve("client-0.pid"));
        stop(directory.resolve("master-0.pid"));
        Exception failure = null;
        try (java.util.stream.Stream<Path> paths = Files.list(directory)) {
            for (Path file : (Iterable<Path>) paths.filter(path -> path.toString().endsWith(".pid"))::iterator) {
                try {
                    stop(file);
                } catch (Exception e) {
                    if (failure == null) {
                        failure = e;
                    } else {
                        failure.addSuppressed(e);
                    }
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}
