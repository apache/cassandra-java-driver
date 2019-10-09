/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.graph;

import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.oss.driver.api.core.session.Session;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/**
 * A session that has the ability to execute DSE Graph requests.
 *
 * <p>Generally this interface won't be referenced directly in an application; instead, you should
 * use {@link DseSession}, which is a combination of this interface and many others for a more
 * integrated usage of DataStax Enterprise's multi-model database via a single entry point. However,
 * it is still possible to cast a {@code DseSession} to a {@code GraphSession} to only expose the
 * DSE Graph execution methods.
 */
public interface GraphSession extends Session {

  /**
   * Executes a graph statement synchronously (the calling thread blocks until the result becomes
   * available).
   *
   * <p>The driver provides different kinds of graph statements:
   *
   * <ul>
   *   <li>{@link FluentGraphStatement} (recommended): wraps a fluent TinkerPop {@linkplain
   *       org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal traversal};
   *   <li>{@link BatchGraphStatement}: groups together multiple mutating traversals ({@code
   *       g.addV()/g.addE()}) inside a single transaction and avoids multiple client-server
   *       round-trips. Improves performance in data ingestion scenarios;
   *   <li>{@link ScriptGraphStatement}: wraps a Gremlin-groovy script provided as a plain Java
   *       string. Required for administrative queries such as creating/dropping a graph,
   *       configuration and schema.
   * </ul>
   *
   * @see GraphResultSet
   */
  @NonNull
  default GraphResultSet execute(@NonNull GraphStatement<?> graphStatement) {
    return Objects.requireNonNull(
        execute(graphStatement, GraphStatement.SYNC),
        "The graph processor should never return a null result");
  }

  /**
   * Executes a graph statement asynchronously (the call returns as soon as the statement was sent,
   * generally before the result is available).
   *
   * @see #execute(GraphStatement)
   * @see AsyncGraphResultSet
   */
  @NonNull
  default CompletionStage<AsyncGraphResultSet> executeAsync(
      @NonNull GraphStatement<?> graphStatement) {
    return Objects.requireNonNull(
        execute(graphStatement, GraphStatement.ASYNC),
        "The graph processor should never return a null result");
  }
}
