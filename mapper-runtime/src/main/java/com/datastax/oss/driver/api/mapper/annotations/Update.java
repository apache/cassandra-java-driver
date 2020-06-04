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
package com.datastax.oss.driver.api.mapper.annotations;

import com.datastax.dse.driver.api.core.cql.reactive.ReactiveResultSet;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.api.mapper.result.MapperResultProducer;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * Annotates a {@link Dao} method that updates one or more instances of an {@link Entity}-annotated
 * class.
 *
 * <p>Example:
 *
 * <pre>
 * &#64;Dao
 * public interface ProductDao {
 *   &#64;Update
 *   void update(Product product);
 * }
 * </pre>
 *
 * <h3>Parameters</h3>
 *
 * <p>The first parameter must be an entity instance. All of its non-PK properties will be
 * interpreted as values to update.
 *
 * <ul>
 *   <li>If {@link #customWhereClause()} is empty, the mapper defaults to an update by primary key
 *       (partition key + clustering columns). The WHERE clause is generated automatically, and
 *       bound with the PK components of the provided entity instance. The query will update at most
 *       one row.
 *   <li>If {@link #customWhereClause()} is not empty, it completely replaces the WHERE clause. If
 *       the provided string contains placeholders, the method must have corresponding additional
 *       parameters (same name, and a compatible Java type):
 *       <pre>
 * &#64;Update(customWhereClause = "description LIKE :searchString")
 * void updateIfDescriptionMatches(Product product, String searchString);
 *       </pre>
 *       The PK components of the provided entity are ignored. Multiple rows may be updated.
 * </ul>
 *
 * <p>If the query has a {@linkplain #ttl() TTL} or {@linkplain #timestamp() timestamp} with
 * placeholders, the method must have corresponding additional parameters (same name, and a
 * compatible Java type):
 *
 * <pre>
 * &#64;Update(ttl = ":ttl")
 * void updateWithTtl(Product product, int ttl);
 * </pre>
 *
 * <pre>
 * &#64;Update(timestamp = ":timestamp")
 * void updateWithTimestamp(Product product, long timestamp);
 * </pre>
 *
 * <p>A {@link Function Function&lt;BoundStatementBuilder, BoundStatementBuilder&gt;} or {@link
 * UnaryOperator UnaryOperator&lt;BoundStatementBuilder&gt;} can be added as the <b>last</b>
 * parameter. It will be applied to the statement before execution. This allows you to customize
 * certain aspects of the request (page size, timeout, etc) at runtime.
 *
 * <h3>Return type</h3>
 *
 * <p>The method can return:
 *
 * <ul>
 *   <li>{@code void}.
 *   <li>a {@code boolean} or {@link Boolean}, which will be mapped to {@link
 *       ResultSet#wasApplied()}. This is intended for conditional queries.
 *       <pre>
 * &#64;Update(ifExists = true)
 * boolean updateIfExists(Product product);
 *       </pre>
 *   <li>a {@link ResultSet}. The method will return the raw query result, without any conversion.
 *       This is intended for queries with custom IF clauses; when those queries are not applied,
 *       they return the actual values of the tested columns.
 *       <pre>
 * &#64;Update(customIfClause = "description = :expectedDescription")
 * ResultSet updateIfDescriptionMatches(Product product, String expectedDescription);
 * // if the condition fails, the result set will contain columns '[applied]' and 'description'
 *       </pre>
 *   <li>a {@link BoundStatement}. This is intended for queries where you will execute this
 *       statement later or in a batch:
 *       <pre>
 * &#64;Update
 * BoundStatement update(Product product);
 *      </pre>
 *   <li>a {@link CompletionStage} or {@link CompletableFuture} of any of the above. The mapper will
 *       execute the query asynchronously. Note that for result sets, you need to switch to the
 *       asynchronous equivalent {@link AsyncResultSet}.
 *       <pre>
 * &#64;Update
 * CompletionStage&lt;Void&gt; update(Product product);
 *
 * &#64;Update(ifExists = true)
 * CompletableFuture&lt;Boolean&gt; updateIfExists(Product product);
 *
 * &#64;Update(customIfClause = "description = :expectedDescription")
 * CompletableFuture&lt;AsyncResultSet&gt; updateIfDescriptionMatches(Product product, String expectedDescription);
 *       </pre>
 *   <li>a {@link ReactiveResultSet}.
 *       <pre>
 * &#64;Update
 * ReactiveResultSet updateReactive(Product product);
 *       </pre>
 *   <li>a {@linkplain MapperResultProducer custom type}.
 * </ul>
 *
 * <h3>Target keyspace and table</h3>
 *
 * <p>If a keyspace was specified when creating the DAO (see {@link DaoFactory}), then the generated
 * query targets that keyspace. Otherwise, it doesn't specify a keyspace, and will only work if the
 * mapper was built from a {@link Session} that has a {@linkplain
 * SessionBuilder#withKeyspace(CqlIdentifier) default keyspace} set.
 *
 * <p>If a table was specified when creating the DAO, then the generated query targets that table.
 * Otherwise, it uses the default table name for the entity (which is determined by the name of the
 * entity class and the naming convention).
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Update {

  /**
   * A custom WHERE clause for the UPDATE query.
   *
   * <p>If this is not empty, it completely replaces the WHERE clause in the generated query. Note
   * that the provided string <b>must not</b> contain the {@code WHERE} keyword.
   *
   * <p>This clause can contain placeholders that will be bound with the method's parameters; see
   * the top-level javadocs of this class for more explanations.
   */
  String customWhereClause() default "";

  /**
   * Whether to append an IF EXISTS clause at the end of the generated UPDATE query.
   *
   * <p>This is mutually exclusive with {@link #customIfClause()} (if both are set, the mapper
   * processor will generate a compile-time error).
   */
  boolean ifExists() default false;

  /**
   * A custom IF clause for the UPDATE query.
   *
   * <p>This is mutually exclusive with {@link #ifExists()} (if both are set, the mapper processor
   * will generate a compile-time error).
   *
   * <p>If this is not empty, it gets added to the generated query. Note that the provided string
   * <b>must not</b> contain the {@code IF} keyword.
   *
   * <p>This clause can contain placeholders that will be bound with the method's parameters; see
   * the top-level javadocs of this class for more explanations.
   */
  String customIfClause() default "";

  /**
   * The TTL (time to live) to use in the generated INSERT query.
   *
   * <p>If this starts with ":", it is interpreted as a named placeholder (that must have a
   * corresponding parameter in the method signature). Otherwise, it must be a literal integer value
   * (representing a number of seconds).
   *
   * <p>If the placeholder name is invalid or the literal can't be parsed as an integer (according
   * to the rules of {@link Integer#parseInt(String)}), the mapper will issue a compile-time
   * warning.
   */
  String ttl() default "";

  /**
   * The timestamp to use in the generated INSERT query.
   *
   * <p>If this starts with ":", it is interpreted as a named placeholder (that must have a
   * corresponding parameter in the method signature). Otherwise, it must be literal long value
   * (representing a number of microseconds since epoch).
   *
   * <p>If the placeholder name is invalid or the literal can't be parsed as a long (according to
   * the rules of {@link Long#parseLong(String)}), the mapper will issue a compile-time warning.
   */
  String timestamp() default "";

  /**
   * How to handle null entity properties during the update.
   *
   * <p>This defaults either to the {@link DefaultNullSavingStrategy DAO-level strategy} (if set),
   * or {@link NullSavingStrategy#DO_NOT_SET}.
   */
  NullSavingStrategy nullSavingStrategy() default NullSavingStrategy.DO_NOT_SET;
}
