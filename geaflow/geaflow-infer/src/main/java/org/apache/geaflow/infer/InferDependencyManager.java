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

package org.apache.geaflow.infer;

import static org.apache.geaflow.infer.util.InferFileUtils.REQUIREMENTS_TXT;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.infer.util.InferFileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InferDependencyManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(InferDependencyManager.class);

    private static final String ENV_RUNNER_SH = "infer/env/install-infer-env.sh";

    private static final String INFER_RUNTIME_PATH = "infer/inferRuntime";

    private static final String FILE_IN_JAR_PREFIX = "/";

    private static final String REQUIREMENTS_PADDLE = "infer/inferRuntime/requirements_paddle.txt";

    private final InferEnvironmentContext environmentContext;

    private final Configuration config;

    private String buildInferEnvShellPath;
    private String inferEnvRequirementsPath;

    public InferDependencyManager(InferEnvironmentContext environmentContext) {
        this.environmentContext = environmentContext;
        this.config = environmentContext.getJobConfig();
        init();
    }

    private void init() {
        List<String> inferRuntimeFiles = buildInferRuntimeFiles();
        for (String inferRuntimeFile : inferRuntimeFiles) {
            InferFileUtils.copyInferFileByURL(environmentContext.getInferFilesDirectory(), inferRuntimeFile);
        }
        String pythonFilesDirectory = environmentContext.getInferFilesDirectory();
        InferFileUtils.prepareInferFilesFromJars(pythonFilesDirectory);
        prepareRequirementsFile(pythonFilesDirectory);
        this.inferEnvRequirementsPath = pythonFilesDirectory + File.separator + REQUIREMENTS_TXT;
        this.buildInferEnvShellPath = InferFileUtils.copyInferFileByURL(environmentContext.getVirtualEnvDirectory(), ENV_RUNNER_SH);
    }

    public String getBuildInferEnvShellPath() {
        return buildInferEnvShellPath;
    }

    public String getInferEnvRequirementsPath() {
        return inferEnvRequirementsPath;
    }

    private List<String> buildInferRuntimeFiles() {
        List<String> runtimeFiles;
        try {
            List<Path> filePaths = InferFileUtils.getPathsFromResourceJAR(INFER_RUNTIME_PATH);
            runtimeFiles = filePaths.stream().map(path -> {
                String filePath = path.toString();
                if (filePath.startsWith(FILE_IN_JAR_PREFIX)) {
                    filePath = filePath.substring(1);
                }
                LOGGER.info("infer runtime file name is {}", filePath);
                return filePath;
            }).collect(Collectors.toList());
        } catch (Exception e) {
            LOGGER.error("get infer runtime files error", e);
            throw new GeaflowRuntimeException("get infer runtime files failed", e);
        }
        return runtimeFiles;
    }

    private void prepareRequirementsFile(String pythonFilesDirectory) {
        File requirementsFile = new File(pythonFilesDirectory, REQUIREMENTS_TXT);
        String customRequirementsPath = config.getString(FrameworkConfigKeys.INFER_ENV_REQUIREMENTS_PATH);
        if (customRequirementsPath != null && !customRequirementsPath.isEmpty()) {
            overrideRequirements(requirementsFile, customRequirementsPath);
            return;
        }
        if (requirementsFile.exists()) {
            return;
        }
        String frameworkType = config.getString(FrameworkConfigKeys.INFER_FRAMEWORK_TYPE);
        if ("PADDLE".equalsIgnoreCase(frameworkType)) {
            copyResourceRequirements(requirementsFile, REQUIREMENTS_PADDLE);
        }
    }

    private void overrideRequirements(File requirementsFile, String sourcePath) {
        try {
            if (sourcePath.startsWith("http://") || sourcePath.startsWith("https://")) {
                FileUtils.copyURLToFile(new URL(sourcePath), requirementsFile);
            } else {
                FileUtils.copyFile(new File(sourcePath), requirementsFile);
            }
            LOGGER.info("override requirements.txt from {}", sourcePath);
        } catch (IOException e) {
            throw new GeaflowRuntimeException("copy custom requirements file failed", e);
        }
    }

    private void copyResourceRequirements(File requirementsFile, String resourcePath) {
        String copiedPath = InferFileUtils.copyInferFileByURL(requirementsFile.getParent(), resourcePath);
        File source = new File(copiedPath);
        if (requirementsFile.exists()) {
            requirementsFile.delete();
        }
        boolean renamed = source.renameTo(requirementsFile);
        if (!renamed) {
            throw new GeaflowRuntimeException("rename requirements file failed: " + source.getAbsolutePath());
        }
        LOGGER.info("use default requirements from resource {}", resourcePath);
    }
}
