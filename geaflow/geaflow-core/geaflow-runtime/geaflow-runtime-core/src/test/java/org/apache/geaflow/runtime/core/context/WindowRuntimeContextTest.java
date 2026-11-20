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

package org.apache.geaflow.runtime.core.context;

import java.util.Arrays;
import java.util.Collections;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.api.function.RichFunction;
import org.apache.geaflow.api.function.io.SinkFunction;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.task.TaskArgs;
import org.apache.geaflow.metrics.common.MetricGroupRegistry;
import org.apache.geaflow.model.record.BatchRecord;
import org.apache.geaflow.model.record.RecordArgs;
import org.apache.geaflow.operator.impl.window.FilterOperator;
import org.apache.geaflow.operator.impl.window.SinkOperator;
import org.apache.geaflow.processor.impl.window.OneInputProcessor;
import org.testng.Assert;
import org.testng.annotations.Test;

public class WindowRuntimeContextTest {
    @Test
    public void testChainedSinkSeesCurrentAndReplayedWindow() {
        Configuration config = new Configuration();
        config.put(ExecutionConfigKeys.REPORTER_LIST.getKey(), "");
        MetricGroupRegistry.getInstance(config);
        DefaultRuntimeContext context = DefaultRuntimeContext.build(config)
            .setTaskArgs(new TaskArgs(1, 0, "window-context", 1, 1024, 0)).setWindowId(1);
        WindowSink sink = new WindowSink();
        FilterOperator<Integer> filter = new FilterOperator<>(value -> value > 0);
        filter.addNextOperator(new SinkOperator<>(sink));
        OneInputProcessor<Integer> processor = new OneInputProcessor<>(filter);
        processor.open(Collections.emptyList(), context);
        try {
            // Advance and then replay an earlier window after rollback. Never use a local counter.
            for (long window : Arrays.asList(1L, 3L, 5L, 3L)) {
                processor.init(window);
                processor.processElement(new BatchRecord<>(new RecordArgs(window), Collections.singletonList(42).iterator()));
                Assert.assertEquals(sink.observedWindow, window);
                processor.finish(window);
            }
        } finally {
            processor.close();
        }
    }

    private static final class WindowSink extends RichFunction implements SinkFunction<Integer> {
        private RuntimeContext context;
        private long observedWindow;

        @Override
        public void open(RuntimeContext context) {
            this.context = context;
        }

        @Override
        public void write(Integer value) {
            observedWindow = context.getWindowId();
        }

        @Override
        public void close() {
        }
    }
}
