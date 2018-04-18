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
package com.datastax.oss.driver.api.querybuilder;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Statement;

/**
 * End state for the query builder DSL, which allows the generation of a CQL query.
 *
 * <p>The API returns this type as soon as there is enough information for a minimal query (in most
 * cases, it's still possible to call more methods to keep building).
 */
public interface BuildableQuery {

  /**
   * Builds the CQL query as a raw string.
   *
   * <p>Use this if you plan to pass the query to {@link CqlSession#execute(String)} or {@link
   * CqlSession#prepare(String)} without any further customization.
   */
  String asCql();

  /**
   * Builds the CQL query and wraps it in a simple statement.
   *
   * <p>This is a similar to:
   *
   * <pre>{@code
   * SimpleStatement.newInstance(asCql())
   * }</pre>
   *
   * In addition, some query implementation might try to infer additional statement properties (such
   * as {@link Statement#isIdempotent()}).
   */
  default SimpleStatement build() {
    return SimpleStatement.newInstance(asCql());
  }

  /**
   * Builds the CQL query and wraps it in a simple statement builder.
   *
   * <p>This is equivalent to {@link #build()}, but the builder might be slightly more efficient if
   * you plan to customize multiple properties on the statement, for example:
   *
   * <pre>{@code
   * SimpleStatementBuilder builder =
   *     selectFrom("foo")
   *         .all()
   *         .whereColumn("k").isEqualTo(bindMarker("k"))
   *         .whereColumn("c").isLessThan(bindMarker("c"))
   *         .builder();
   * SimpleStatement statement =
   *     builder.addNamedValue("k", 1).addNamedValue("c", 2).withTracing().build();
   * }</pre>
   */
  default SimpleStatementBuilder builder() {
    return SimpleStatement.builder(asCql());
  }
}
