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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * Annotates a {@link Dao} method that inserts an instance of an {@link Entity}-annotated class.
 *
 * <p>Example:
 *
 * <pre>
 * &#64;Dao
 * public interface ProductDao {
 *   &#64;Insert
 *   void insert(Product product);
 * }
 * </pre>
 *
 * <h3>Parameters</h3>
 *
 * The first parameter must be the entity to insert.
 *
 * <p>If the query has a {@linkplain #ttl() TTL} and/or {@linkplain #timestamp() timestamp} with
 * placeholders, the method must have corresponding additional parameters (same name, and a
 * compatible Java type):
 *
 * <pre>
 * &#64;Insert(ttl = ":ttl")
 * void insertWithTtl(Product product, int ttl);
 * </pre>
 *
 * <p>A {@link Function Function&lt;BoundStatementBuilder, BoundStatementBuilder&gt;} or {@link
 * UnaryOperator UnaryOperator&lt;BoundStatementBuilder&gt;} can be added as the <b>last</b>
 * parameter. It will be applied to the statement before execution. This allows you to customize
 * certain aspects of the request (page size, timeout, etc) at runtime.
 *
 * <h3>Return type</h3>
 *
 * The method can return:
 *
 * <ul>
 *   <li>{@code void}.
 *   <li>the entity class. This is intended for {@code INSERT ... IF NOT EXISTS} queries. The method
 *       will return {@code null} if the insertion succeeded, or the existing entity if it failed.
 *       <pre>
 * &#64;Insert(ifNotExists = true)
 * Product insertIfNotExists(Product product);
 *       </pre>
 *   <li>an {@link Optional} of the entity class, as a null-safe alternative for {@code INSERT ...
 *       IF NOT EXISTS} queries.
 *       <pre>
 * &#64;Insert(ifNotExists = true)
 * Optional&lt;Product&gt; insertIfNotExists(Product product);
 *       </pre>
 *   <li>a {@code boolean} or {@link Boolean}, which will be mapped to {@link
 *       ResultSet#wasApplied()}. This is intended for IF NOT EXISTS queries:
 *       <pre>
 * &#64;Insert(ifNotExists = true)
 * boolean saveIfNotExists(Product product);
 *       </pre>
 *   <li>a {@link ResultSet}. This is intended for cases where you intend to inspect data associated
 *       with the result, such as {@link ResultSet#getExecutionInfo()}.
 *       <pre>
 * &#64;Insert
 * ResultSet save(Product product);
 *       </pre>
 *   <li>a {@link BoundStatement} This is intended for cases where you intend to execute this
 *       statement later or in a batch:
 *       <pre>
 * &#64;Insert
 * BoundStatement save(Product product);
 *      </pre>
 *   <li>a {@link CompletionStage} or {@link CompletableFuture} of any of the above. The mapper will
 *       execute the query asynchronously.
 *       <pre>
 * &#64;Insert
 * CompletionStage&lt;Void&gt; insert(Product product);
 *
 * &#64;Insert(ifNotExists = true)
 * CompletableFuture&lt;Product&gt; insertIfNotExists(Product product);
 *
 * &#64;Insert(ifNotExists = true)
 * CompletableFuture&lt;Optional&lt;Product&gt;&gt; insertIfNotExists(Product product);
 *       </pre>
 *   <li>a {@link ReactiveResultSet}.
 *       <pre>
 * &#64;Insert
 * ReactiveResultSet insertReactive(Product product);
 *       </pre>
 *   <li>a {@linkplain MapperResultProducer custom type}.
 * </ul>
 *
 * <h3>Target keyspace and table</h3>
 *
 * If a keyspace was specified when creating the DAO (see {@link DaoFactory}), then the generated
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
public @interface Insert {
  /** Whether to append an IF NOT EXISTS clause at the end of the generated INSERT query. */
  boolean ifNotExists() default false;

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
   * The timeout to use in the generated INSERT query. Equivalent to {@code USING TIMEOUT
   * <duration>} clause.
   *
   * <p>If this starts with ":", it is interpreted as a named placeholder (that must have a
   * corresponding parameter in the method signature). Otherwise, it must be a String representing a
   * valid CqlDuration.
   *
   * <p>If the placeholder name is invalid or the literal can't be parsed as a CqlDuration
   * (according to the rules of {@link
   * com.datastax.oss.driver.api.core.data.CqlDuration#from(String)}), the mapper will issue a
   * compile-time error.
   */
  String usingTimeout() default "";

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
   * How to handle null entity properties during the insertion.
   *
   * <p>This defaults either to the {@link DefaultNullSavingStrategy DAO-level strategy} (if set),
   * or {@link NullSavingStrategy#DO_NOT_SET}.
   */
  NullSavingStrategy nullSavingStrategy() default NullSavingStrategy.DO_NOT_SET;
}
