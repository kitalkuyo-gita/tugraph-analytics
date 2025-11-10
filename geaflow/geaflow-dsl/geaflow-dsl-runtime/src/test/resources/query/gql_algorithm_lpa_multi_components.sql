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

CREATE GRAPH lpa_test_graph (
  Vertex v (
    id int ID,
    value int
  ),
  Edge e (
    srcId int SOURCE ID,
    targetId int DESTINATION ID
  )
) WITH (
  storeType='memory',
  shardCount = 2
);

CREATE TABLE v_source (
    id int,
    value int
) WITH (
  type='file',
  geaflow.dsl.file.path = 'resource:///data/lpa_test_vertex.txt'
);

CREATE TABLE e_source (
    src_id int,
    dst_id int
) WITH (
  type='file',
  geaflow.dsl.file.path = 'resource:///data/lpa_test_edge.txt'
);

CREATE TABLE result_tb (
   vid int,
   label int
) WITH (
      type='file',
      geaflow.dsl.file.path='${target}'
);

INSERT INTO lpa_test_graph.v(id, value)
SELECT id, value
FROM v_source;

INSERT INTO lpa_test_graph.e(srcId, targetId)
SELECT src_id, dst_id
FROM e_source;

USE GRAPH lpa_test_graph;

INSERT INTO result_tb
CALL lpa(30) YIELD (vid, label)
RETURN cast (vid as int), cast (label as int)
;