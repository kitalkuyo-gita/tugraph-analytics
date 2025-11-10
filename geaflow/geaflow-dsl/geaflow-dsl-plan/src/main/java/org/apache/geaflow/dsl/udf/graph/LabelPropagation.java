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

package org.apache.geaflow.dsl.udf.graph;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import org.apache.geaflow.dsl.common.algo.AlgorithmUserFunction;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.function.Description;
import org.apache.geaflow.dsl.common.types.GraphSchema;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.model.graph.edge.EdgeDirection;

@Description(name = "lpa", description = "built-in udga for Label Propagation Algorithm")
public class LabelPropagation implements AlgorithmUserFunction<Object, Object> {

    private AlgorithmRuntimeContext<Object, Object> context;
    private String keyFieldName = "label";
    private int iteration = 50;

    @Override
    public void init(AlgorithmRuntimeContext<Object, Object> context, Object[] parameters) {
        this.context = context;
        if (parameters.length > 2) {
            throw new IllegalArgumentException(
                "Only support zero or more arguments, false arguments "
                    + "usage: func([iteration, [keyFieldName]])");
        }
        if (parameters.length > 0) {
            iteration = Integer.parseInt(String.valueOf(parameters[0]));
        }
        if (parameters.length > 1) {
            keyFieldName = String.valueOf(parameters[1]);
        }
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<Object> messages) {
        updatedValues.ifPresent(vertex::setValue);
        
        if (context.getCurrentIterationId() == 1L) {
            // First iteration: initialize label as vertex ID and send to neighbors
            Object initLabel = vertex.getId();
            List<RowEdge> edges = context.loadEdges(EdgeDirection.BOTH);
            sendMessageToNeighbors(edges, initLabel);
            context.updateVertexValue(ObjectRow.create(initLabel));
        } else if (context.getCurrentIterationId() < iteration) {
            // Subsequent iterations: receive messages and update label
            if (!messages.hasNext()) {
                return;
            }
            
            Object maxLabel = messages.next();
            while (messages.hasNext()) {
                Object next = messages.next();
                if (compareLabels(next, maxLabel) > 0) {
                    maxLabel = next;
                }
            }
            
            Row vertexValue = vertex.getValue();
            if (vertexValue != null) {
                Object currentLabel = vertexValue.getField(0, context.getGraphSchema().getIdType());
                // Only update if new label is larger
                if (compareLabels(maxLabel, currentLabel) > 0) {
                    List<RowEdge> edges = context.loadEdges(EdgeDirection.BOTH);
                    sendMessageToNeighbors(edges, maxLabel);
                    context.updateVertexValue(ObjectRow.create(maxLabel));
                }
            } else {
                // No current value, update with max label
                List<RowEdge> edges = context.loadEdges(EdgeDirection.BOTH);
                sendMessageToNeighbors(edges, maxLabel);
                context.updateVertexValue(ObjectRow.create(maxLabel));
            }
        }
    }

    @Override
    public void finish(RowVertex graphVertex, Optional<Row> updatedValues) {
        updatedValues.ifPresent(graphVertex::setValue);
        if (graphVertex.getValue() != null) {
            Object label = graphVertex.getValue().getField(0, context.getGraphSchema().getIdType());
            context.take(ObjectRow.create(graphVertex.getId(), label));
        }
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
            new TableField("vid", graphSchema.getIdType(), false),
            new TableField(keyFieldName, graphSchema.getIdType(), false)
        );
    }

    /**
     * Compare two labels, handling different types properly.
     */
    @SuppressWarnings("unchecked")
    private int compareLabels(Object label1, Object label2) {
        if (label1 == null && label2 == null) {
            return 0;
        }
        if (label1 == null) {
            return -1;
        }
        if (label2 == null) {
            return 1;
        }
        
        // If both are comparable, use their natural ordering
        if (label1 instanceof Comparable && label2 instanceof Comparable) {
            try {
                return ((Comparable<Object>) label1).compareTo(label2);
            } catch (ClassCastException e) {
                // If types are incompatible, compare as strings
                return label1.toString().compareTo(label2.toString());
            }
        }
        
        // Fall back to string comparison
        return label1.toString().compareTo(label2.toString());
    }

    private void sendMessageToNeighbors(List<RowEdge> edges, Object message) {
        for (RowEdge rowEdge : edges) {
            context.sendMessage(rowEdge.getTargetId(), message);
        }
    }
}