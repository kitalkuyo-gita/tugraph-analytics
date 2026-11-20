#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

set -euo pipefail

# Prerequisite: install the JDK 17 Iceberg reactor, including its tests and isolated JAR.
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"
maven_cmd="${MVN:-mvn}"
connector_dir="geaflow/geaflow-dsl/geaflow-dsl-connector"

"$maven_cmd" -B -ntp \
  -pl "$connector_dir/geaflow-dsl-connector-file,$connector_dir/geaflow-dsl-connector-paimon" \
  install -DskipTests -Dmaven.javadoc.skip=true

mixed_classpath=""
for connector in file paimon; do
  module_dir="$connector_dir/geaflow-dsl-connector-$connector"
  "$maven_cmd" -B -ntp -f "$module_dir/pom.xml" dependency:build-classpath \
    -DincludeScope=runtime -Dmdep.outputFile=target/runtime-classpath.txt
  connector_classpath="$repo_root/$module_dir/target/geaflow-dsl-connector-$connector-0.8.0-SNAPSHOT.jar:$(cat "$module_dir/target/runtime-classpath.txt")"
  mixed_classpath="${mixed_classpath:+$mixed_classpath:}$connector_classpath"
done

"$maven_cmd" -B -ntp -Piceberg-jdk17,iceberg-mixed-it \
  -pl "$connector_dir/geaflow-dsl-connector-iceberg" \
  failsafe:integration-test failsafe:verify \
  "-Diceberg.test.mixed.classpath=$mixed_classpath"
