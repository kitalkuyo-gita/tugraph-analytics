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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.DataFile;
import org.testng.Assert;
import org.testng.annotations.Test;

public class IcebergMixedRuntimeIT {
    @Test
    public void testDefaultFilePaimonRuntimeWithIsolatedIceberg() throws Exception {
        String baseline = System.getProperty("iceberg.test.mixed.classpath");
        Assert.assertNotNull(baseline, "Supply the default file/Paimon runtime classpath for iceberg-mixed-it");
        try (IcebergTestFixture fixture = new IcebergTestFixture()) {
            DataFile file = fixture.append(0, 12);
            fixture.positionDelete(file, 2);
            fixture.equalityDelete(7L);
            Properties properties = new Properties();
            properties.putAll(fixture.configuration("mixed-runtime", 7).getConfigMap());
            Path config = fixture.directory.resolve("mixed.properties");
            try (java.io.OutputStream output = Files.newOutputStream(config)) {
                properties.store(output, "Non-secret integration fixture");
            }
            String plugin = Path.of("target/geaflow-dsl-connector-iceberg-0.8.0-SNAPSHOT-isolated.jar").toAbsolutePath().toString();
            String prefix = Path.of("target/test-classes").toAbsolutePath() + java.io.File.pathSeparator
                + org.h2.Driver.class.getProtectionDomain().getCodeSource().getLocation().getPath();
            for (boolean hostFirst : new boolean[]{false, true}) {
                String cp = prefix + java.io.File.pathSeparator
                    + (hostFirst ? baseline + java.io.File.pathSeparator + plugin : plugin + java.io.File.pathSeparator + baseline);
                Path log = fixture.directory.resolve("mixed-" + hostFirst + ".log");
                Process child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin/java").toString(),
                    "-cp", cp, IcebergMixedRuntimeProbe.class.getName(), config.toString(), file.location())
                    .redirectErrorStream(true).redirectOutput(log.toFile()).start();
                if (!child.waitFor(60, TimeUnit.SECONDS)) {
                    child.destroyForcibly();
                    Assert.fail("Mixed runtime probe timed out; hostFirst=" + hostFirst);
                }
                Assert.assertEquals(child.exitValue(), 0, "hostFirst=" + hostFirst + "\n" + Files.readString(log));
            }
        }
    }
}
