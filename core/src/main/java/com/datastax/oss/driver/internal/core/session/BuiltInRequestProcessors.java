/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.session;

import static com.datastax.oss.driver.internal.core.util.Dependency.REACTIVE_STREAMS;
import static com.datastax.oss.driver.internal.core.util.Dependency.TINKERPOP;

import com.datastax.dse.driver.internal.core.cql.continuous.ContinuousCqlRequestAsyncProcessor;
import com.datastax.dse.driver.internal.core.cql.continuous.ContinuousCqlRequestSyncProcessor;
import com.datastax.dse.driver.internal.core.cql.continuous.reactive.ContinuousCqlRequestReactiveProcessor;
import com.datastax.dse.driver.internal.core.cql.reactive.CqlRequestReactiveProcessor;
import com.datastax.dse.driver.internal.core.graph.GraphRequestAsyncProcessor;
import com.datastax.dse.driver.internal.core.graph.GraphRequestSyncProcessor;
import com.datastax.dse.driver.internal.core.graph.GraphSupportChecker;
import com.datastax.dse.driver.internal.core.graph.reactive.ReactiveGraphRequestProcessor;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.cql.CqlPrepareAsyncProcessor;
import com.datastax.oss.driver.internal.core.cql.CqlPrepareSyncProcessor;
import com.datastax.oss.driver.internal.core.cql.CqlRequestAsyncProcessor;
import com.datastax.oss.driver.internal.core.cql.CqlRequestSyncProcessor;
import com.datastax.oss.driver.internal.core.util.DefaultDependencyChecker;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BuiltInRequestProcessors {

  private static final Logger LOG = LoggerFactory.getLogger(BuiltInRequestProcessors.class);

  public static List<RequestProcessor<?, ?>> createDefaultProcessors(DefaultDriverContext context) {
    List<RequestProcessor<?, ?>> processors = new ArrayList<>();
    addBasicProcessors(processors);
    if (DefaultDependencyChecker.isPresent(TINKERPOP)) {
      addGraphProcessors(context, processors);
    } else {
      LOG.debug("Tinkerpop was not found on the classpath: graph extensions will not be available");
    }
    if (DefaultDependencyChecker.isPresent(REACTIVE_STREAMS)) {
      addReactiveProcessors(processors);
    } else {
      LOG.debug(
          "Reactive Streams was not found on the classpath: reactive extensions will not be available");
    }
    if (DefaultDependencyChecker.isPresent(REACTIVE_STREAMS)
        && DefaultDependencyChecker.isPresent(TINKERPOP)) {
      addGraphReactiveProcessors(context, processors);
    }
    return processors;
  }

  public static void addBasicProcessors(List<RequestProcessor<?, ?>> processors) {
    // regular requests (sync and async)
    CqlRequestAsyncProcessor cqlRequestAsyncProcessor = new CqlRequestAsyncProcessor();
    CqlRequestSyncProcessor cqlRequestSyncProcessor =
        new CqlRequestSyncProcessor(cqlRequestAsyncProcessor);
    processors.add(cqlRequestAsyncProcessor);
    processors.add(cqlRequestSyncProcessor);

    // prepare requests (sync and async)
    CqlPrepareAsyncProcessor cqlPrepareAsyncProcessor = new CqlPrepareAsyncProcessor();
    CqlPrepareSyncProcessor cqlPrepareSyncProcessor =
        new CqlPrepareSyncProcessor(cqlPrepareAsyncProcessor);
    processors.add(cqlPrepareAsyncProcessor);
    processors.add(cqlPrepareSyncProcessor);

    // continuous requests (sync and async)
    ContinuousCqlRequestAsyncProcessor continuousCqlRequestAsyncProcessor =
        new ContinuousCqlRequestAsyncProcessor();
    ContinuousCqlRequestSyncProcessor continuousCqlRequestSyncProcessor =
        new ContinuousCqlRequestSyncProcessor(continuousCqlRequestAsyncProcessor);
    processors.add(continuousCqlRequestAsyncProcessor);
    processors.add(continuousCqlRequestSyncProcessor);
  }

  public static void addGraphProcessors(
      DefaultDriverContext context, List<RequestProcessor<?, ?>> processors) {
    GraphRequestAsyncProcessor graphRequestAsyncProcessor =
        new GraphRequestAsyncProcessor(context, new GraphSupportChecker());
    GraphRequestSyncProcessor graphRequestSyncProcessor =
        new GraphRequestSyncProcessor(graphRequestAsyncProcessor);
    processors.add(graphRequestAsyncProcessor);
    processors.add(graphRequestSyncProcessor);
  }

  public static void addReactiveProcessors(List<RequestProcessor<?, ?>> processors) {
    CqlRequestReactiveProcessor cqlRequestReactiveProcessor =
        new CqlRequestReactiveProcessor(new CqlRequestAsyncProcessor());
    ContinuousCqlRequestReactiveProcessor continuousCqlRequestReactiveProcessor =
        new ContinuousCqlRequestReactiveProcessor(new ContinuousCqlRequestAsyncProcessor());
    processors.add(cqlRequestReactiveProcessor);
    processors.add(continuousCqlRequestReactiveProcessor);
  }

  public static void addGraphReactiveProcessors(
      DefaultDriverContext context, List<RequestProcessor<?, ?>> processors) {
    ReactiveGraphRequestProcessor reactiveGraphRequestProcessor =
        new ReactiveGraphRequestProcessor(
            new GraphRequestAsyncProcessor(context, new GraphSupportChecker()));
    processors.add(reactiveGraphRequestProcessor);
  }
}
