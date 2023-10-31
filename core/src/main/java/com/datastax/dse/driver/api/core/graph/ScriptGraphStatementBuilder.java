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
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import net.jcip.annotations.NotThreadSafe;

/**
 * A builder to create a script graph statement.
 *
 * <p>This class is mutable and not thread-safe.
 */
@NotThreadSafe
public class ScriptGraphStatementBuilder
    extends GraphStatementBuilderBase<ScriptGraphStatementBuilder, ScriptGraphStatement> {

  private String script;
  private Boolean isSystemQuery;
  private final Map<String, Object> queryParams;

  public ScriptGraphStatementBuilder() {
    this.queryParams = Maps.newHashMap();
  }

  public ScriptGraphStatementBuilder(String script) {
    this.script = script;
    this.queryParams = Maps.newHashMap();
  }

  public ScriptGraphStatementBuilder(ScriptGraphStatement template) {
    super(template);
    this.script = template.getScript();
    this.queryParams = Maps.newHashMap(template.getQueryParams());
    this.isSystemQuery = template.isSystemQuery();
  }

  @Nonnull
  public ScriptGraphStatementBuilder setScript(@Nonnull String script) {
    this.script = script;
    return this;
  }

  /** @see ScriptGraphStatement#isSystemQuery() */
  @Nonnull
  public ScriptGraphStatementBuilder setSystemQuery(@Nullable Boolean isSystemQuery) {
    this.isSystemQuery = isSystemQuery;
    return this;
  }

  /**
   * Set a value for a parameter defined in the script query.
   *
   * @see ScriptGraphStatement#setQueryParam(String, Object)
   */
  @Nonnull
  public ScriptGraphStatementBuilder setQueryParam(@Nonnull String name, @Nullable Object value) {
    this.queryParams.put(name, value);
    return this;
  }

  /**
   * Set multiple values for named parameters defined in the script query.
   *
   * @see ScriptGraphStatement#setQueryParam(String, Object)
   */
  @Nonnull
  public ScriptGraphStatementBuilder setQueryParams(@Nonnull Map<String, Object> params) {
    this.queryParams.putAll(params);
    return this;
  }

  /**
   * Removes a parameter.
   *
   * <p>This is useful if the builder was {@linkplain
   * ScriptGraphStatement#builder(ScriptGraphStatement) initialized with a template statement} that
   * has more parameters than desired.
   *
   * @see ScriptGraphStatement#setQueryParam(String, Object)
   * @see #clearQueryParams()
   */
  @Nonnull
  public ScriptGraphStatementBuilder removeQueryParam(@Nonnull String name) {
    this.queryParams.remove(name);
    return this;
  }

  /** Clears all the parameters previously added to this builder. */
  public ScriptGraphStatementBuilder clearQueryParams() {
    this.queryParams.clear();
    return this;
  }

  @Nonnull
  @Override
  public ScriptGraphStatement build() {
    Preconditions.checkNotNull(this.script, "Script hasn't been defined in this builder.");
    return new DefaultScriptGraphStatement(
        this.script,
        this.queryParams,
        this.isSystemQuery,
        isIdempotent,
        timeout,
        node,
        timestamp,
        executionProfile,
        executionProfileName,
        buildCustomPayload(),
        graphName,
        traversalSource,
        subProtocol,
        consistencyLevel,
        readConsistencyLevel,
        writeConsistencyLevel);
  }
}
