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
package com.datastax.dse.driver.api.core.cql.continuous;

import com.datastax.dse.driver.internal.core.cql.continuous.ContinuousCqlRequestAsyncProcessor;
import com.datastax.dse.driver.internal.core.cql.continuous.ContinuousCqlRequestSyncProcessor;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.session.Session;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/**
 * A session that has the ability to execute continuous paging queries.
 *
 * <p>Continuous paging is a new method of streaming bulk amounts of records from Datastax
 * Enterprise (DSE) to the Datastax Java Driver, available since DSE 5.1. It is mainly intended to
 * be leveraged by <a
 * href="https://docs.datastax.com/en/dse/5.1/dse-dev/datastax_enterprise/analytics/analyticsTOC.html">DSE
 * Analytics</a> and Apache Spark™, or by any similar analytics tool that needs to read large
 * portions of a table in one single operation, as quick and reliably as possible.
 *
 * <p>Continuous paging provides the best performance improvement against regular paging when the
 * following conditions are met:
 *
 * <ol>
 *   <li>The statement must target a single partition or a token range owned by one single replica;
 *       in practice, this means that the statement must have either a {@linkplain
 *       Statement#setRoutingKey(ByteBuffer) routing key} or a {@linkplain
 *       Statement#setRoutingToken(Token) routing token} set;
 *   <li>The coordinator must be a replica; in practice, this is usually achieved by using
 *       token-aware routing (if you are using the driver's default {@link LoadBalancingPolicy},
 *       then this condition is met);
 *   <li>The consistency level must be {@link DefaultConsistencyLevel#ONE ONE} (or {@link
 *       DefaultConsistencyLevel#LOCAL_ONE LOCAL_ONE}).
 * </ol>
 *
 * <p>If the above conditions are met, the coordinator will be able to optimize the read path and
 * serve results from local data, thus significantly improving response times; if however these
 * conditions cannot be met, continuous paging would still work, but response times wouldn't be
 * significantly better than those of regular paging anymore.
 *
 * @see <a
 *     href="https://docs.datastax.com/en/dse/6.7/dse-dev/datastax_enterprise/config/configCassandra_yaml.html#configCassandra_yaml__continous-paging">Continuous
 *     paging options in cassandra.yaml configuration file</a>
 * @see <a
 *     href="https://www.datastax.com/dev/blog/dse-continuous-paging-tuning-and-support-guide">DSE
 *     Continuous Paging Tuning and Support Guide</a>
 */
public interface ContinuousSession extends Session {

  /**
   * Executes the provided query with continuous paging synchronously.
   *
   * <p>This method takes care of chaining the successive results into a convenient iterable,
   * provided that you always access the result from the same thread. For more flexibility, consider
   * using the {@linkplain #executeContinuouslyAsync(Statement) asynchronous variant} of this method
   * instead.
   *
   * <p>See {@link ContinuousSession} for more explanations about continuous paging.
   *
   * <p>This feature is only available with Datastax Enterprise. Executing continuous queries
   * against an Apache Cassandra&copy; cluster will result in a runtime error.
   *
   * @param statement the query to execute.
   * @return a synchronous iterable on the results.
   */
  @NonNull
  default ContinuousResultSet executeContinuously(@NonNull Statement<?> statement) {
    return Objects.requireNonNull(
        execute(statement, ContinuousCqlRequestSyncProcessor.CONTINUOUS_RESULT_SYNC));
  }

  /**
   * Executes the provided query with continuous paging asynchronously.
   *
   * <p>The server will push all requested pages asynchronously, according to the options defined in
   * the current execution profile. The client should consume all pages as quickly as possible, to
   * avoid blocking the server for too long. The server will adjust the rate according to the client
   * speed, but it will give up if the client does not consume any pages in a period of time equal
   * to the read request timeout.
   *
   * <p>See {@link ContinuousSession} for more explanations about continuous paging.
   *
   * <p>This feature is only available with Datastax Enterprise. Executing continuous queries
   * against an Apache Cassandra&copy; cluster will result in a runtime error.
   *
   * @param statement the query to execute.
   * @return a future to the first asynchronous result.
   */
  @NonNull
  default CompletionStage<ContinuousAsyncResultSet> executeContinuouslyAsync(
      @NonNull Statement<?> statement) {
    return Objects.requireNonNull(
        execute(statement, ContinuousCqlRequestAsyncProcessor.CONTINUOUS_RESULT_ASYNC));
  }
}
