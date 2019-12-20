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
package com.datastax.dse.driver.internal.core.graph;

import com.datastax.dse.driver.api.core.graph.AsyncGraphResultSet;
import com.datastax.dse.driver.api.core.graph.BatchGraphStatement;
import com.datastax.dse.driver.api.core.graph.FluentGraphStatement;
import com.datastax.dse.driver.api.core.graph.GraphStatement;
import com.datastax.dse.driver.api.core.graph.ScriptGraphStatement;
import com.datastax.dse.driver.internal.core.graph.binary.GraphBinaryModule;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.session.RequestProcessor;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.CompletionStage;
import net.jcip.annotations.ThreadSafe;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;

@ThreadSafe
public class GraphRequestAsyncProcessor
    implements RequestProcessor<GraphStatement<?>, CompletionStage<AsyncGraphResultSet>> {

  private final GraphBinaryModule graphBinaryModule;
  private final GraphSupportChecker graphSupportChecker;

  public GraphRequestAsyncProcessor(
      DefaultDriverContext context, GraphSupportChecker graphSupportChecker) {
    TypeSerializerRegistry typeSerializerRegistry =
        GraphBinaryModule.createDseTypeSerializerRegistry(context);
    this.graphBinaryModule =
        new GraphBinaryModule(
            new GraphBinaryReader(typeSerializerRegistry),
            new GraphBinaryWriter(typeSerializerRegistry));
    this.graphSupportChecker = graphSupportChecker;
  }

  @NonNull
  public GraphBinaryModule getGraphBinaryModule() {
    return graphBinaryModule;
  }

  @Override
  public boolean canProcess(Request request, GenericType<?> resultType) {
    return (request instanceof ScriptGraphStatement
            || request instanceof FluentGraphStatement
            || request instanceof BatchGraphStatement
            || request instanceof BytecodeGraphStatement)
        && resultType.equals(GraphStatement.ASYNC);
  }

  @Override
  public CompletionStage<AsyncGraphResultSet> process(
      GraphStatement<?> request,
      DefaultSession session,
      InternalDriverContext context,
      String sessionLogPrefix) {

    if (graphSupportChecker.isPagingEnabled(request, context)) {
      return new ContinuousGraphRequestHandler(
              request,
              session,
              context,
              sessionLogPrefix,
              getGraphBinaryModule(),
              graphSupportChecker)
          .handle();
    } else {
      return new GraphRequestHandler(
              request,
              session,
              context,
              sessionLogPrefix,
              getGraphBinaryModule(),
              graphSupportChecker)
          .handle();
    }
  }

  @Override
  public CompletionStage<AsyncGraphResultSet> newFailure(RuntimeException error) {
    return CompletableFutures.failedFuture(error);
  }
}
