/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.dse.driver.api.core.graph;

import com.datastax.dse.driver.internal.core.graph.DefaultScriptGraphStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Collections;
import java.util.Map;

/**
 * A graph statement that uses a Gremlin-groovy script the query.
 *
 * <p>These statements are generally used for DSE Graph set-up queries, such as creating or dropping
 * a graph, or defining a graph schema. For graph traversals, we recommend using {@link
 * FluentGraphStatement} instead. To do bulk data ingestion in graph, we recommend using {@link
 * BatchGraphStatement} instead.
 *
 * <p>Typical usage:
 *
 * <pre>{@code
 * ScriptGraphStatement statement = ScriptGraphStatement.newInstance("schema.propertyKey('age').Int().create()");
 *
 * GraphResultSet graphResultSet = dseSession.execute(statement);
 * }</pre>
 */
public interface ScriptGraphStatement extends GraphStatement<ScriptGraphStatement> {

  /** Create a new instance from the given script. */
  @NonNull
  static ScriptGraphStatement newInstance(@NonNull String script) {
    return new DefaultScriptGraphStatement(
        script,
        NullAllowingImmutableMap.of(),
        null,
        null,
        null,
        null,
        Statement.NO_DEFAULT_TIMESTAMP,
        null,
        null,
        Collections.emptyMap(),
        null,
        null,
        null,
        null,
        null,
        null);
  }

  /**
   * Create a builder object to start creating a new instance from the given script.
   *
   * <p>Note that this builder is mutable and not thread-safe.
   */
  @NonNull
  static ScriptGraphStatementBuilder builder(@NonNull String script) {
    return new ScriptGraphStatementBuilder(script);
  }

  /**
   * Create a builder helper object to start creating a new instance with an existing statement as a
   * template. The script and options set on the template will be copied for the new statement at
   * the moment this method is called.
   *
   * <p>Note that this builder is mutable and not thread-safe.
   */
  @NonNull
  static ScriptGraphStatementBuilder builder(@NonNull ScriptGraphStatement template) {
    return new ScriptGraphStatementBuilder(template);
  }

  /** The Gremlin-groovy script representing the graph query. */
  @NonNull
  String getScript();

  /**
   * Whether the statement is a system query, or {@code null} if it defaults to the value defined in
   * the configuration.
   *
   * @see #setSystemQuery(Boolean)
   */
  @Nullable
  Boolean isSystemQuery();

  /**
   * Defines if this statement is a system query.
   *
   * <p>Script statements that access the {@code system} variable <b>must not</b> specify a graph
   * name (otherwise {@code system} is not available). However, if your application executes a lot
   * of non-system statements, it is convenient to configure the graph name in your configuration to
   * avoid repeating it every time. This method allows you to ignore that global graph name for a
   * specific statement.
   *
   * <p>This property is the programmatic equivalent of the configuration option {@code
   * basic.graph.is-system-query}, and takes precedence over it. That is, if this property is
   * non-null, then the configuration will be ignored.
   *
   * <p>The driver's built-in implementation is immutable, and returns a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance.
   *
   * @param newValue {@code true} to mark this statement as a system query (the driver will ignore
   *     any graph name set on the statement or the configuration); {@code false} to mark it as a
   *     non-system query; {@code null} to default to the value defined in the configuration.
   * @see #isSystemQuery()
   */
  @NonNull
  ScriptGraphStatement setSystemQuery(@Nullable Boolean newValue);

  /**
   * The query parameters to send along the request.
   *
   * @see #setQueryParam(String, Object)
   */
  @NonNull
  Map<String, Object> getQueryParams();

  /**
   * Set a value for a parameter defined in the Groovy script.
   *
   * <p>The script engine in the DSE Graph server allows to define parameters in a Groovy script and
   * set the values of these parameters as a binding. Defining parameters allows to re-use scripts
   * and only change their parameters values, which improves the performance of the script executed,
   * so defining parameters is encouraged; however, for optimal Graph traversal performance, we
   * recommend either using {@link BatchGraphStatement}s for data ingestion, or {@link
   * FluentGraphStatement} for normal traversals.
   *
   * <p>Parameters in a Groovy script are always named; unlike CQL, they are not prefixed by a
   * column ({@code :}).
   *
   * <p>The driver's built-in implementation is immutable, and returns a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance.
   * If many parameters are to be set in a query, it is recommended to create the statement with
   * {@link #builder(String)} instead.
   *
   * @param name the name of the parameter defined in the script. If the statement already had a
   *     binding for this name, it gets replaced.
   * @param value the value that will be transmitted with the request.
   */
  @NonNull
  ScriptGraphStatement setQueryParam(@NonNull String name, @Nullable Object value);

  /**
   * Removes a binding for the given name from this statement.
   *
   * <p>If the statement did not have such a binding, this method has no effect and returns the same
   * statement instance. Otherwise, the driver's built-in implementation returns a new instance
   * (however custom implementations may choose to be mutable and return the same instance).
   *
   * @see #setQueryParam(String, Object)
   */
  @NonNull
  ScriptGraphStatement removeQueryParam(@NonNull String name);
}
