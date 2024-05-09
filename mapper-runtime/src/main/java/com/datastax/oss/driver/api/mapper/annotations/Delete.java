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
package com.datastax.oss.driver.api.mapper.annotations;

import com.datastax.dse.driver.api.core.cql.reactive.ReactiveResultSet;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
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
 * Annotates a {@link Dao} method that deletes an instance of an {@link Entity}-annotated class.
 *
 * <p>Example:
 *
 * <pre>
 * &#64;Dao
 * public interface ProductDao {
 *   &#64;Delete
 *   void delete(Product product);
 * }
 * </pre>
 *
 * <h3>Parameters</h3>
 *
 * The method can operate either on an entity instance, or on a primary key (partition key +
 * clustering columns).
 *
 * <p>In the latter case, the parameters must match the types of the primary key columns, in the
 * exact order (which is defined by the integer values of the {@link PartitionKey} and {@link
 * ClusteringColumn} annotations in the entity class). The parameter names don't necessarily need to
 * match the names of the columns. In addition, because the entity class can't be inferred from the
 * method signature, it must be specified in the annotation with {@link #entityClass()}:
 *
 * <pre>
 * &#64;Delete(entityClass = Product.class)
 * void deleteById(UUID productId);
 * </pre>
 *
 * An {@linkplain #customIfClause() optional IF clause} can be appended to the generated query. It
 * can contain placeholders, for which the method must have corresponding parameters (same name, and
 * a compatible Java type):
 *
 * <pre>
 * &#64;Delete(entityClass = Product.class, customIfClause = "description = :expectedDescription")
 * ResultSet deleteIfDescriptionMatches(UUID productId, String expectedDescription);
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
 *   <li>a {@code boolean} or {@link Boolean}, which will be mapped to {@link
 *       ResultSet#wasApplied()}. This is intended for IF EXISTS queries:
 *       <pre>
 * &#64;Delete(ifExists = true)
 * boolean deleteIfExists(Product product);
 *       </pre>
 *   <li>a {@link ResultSet}. This is intended for queries with custom IF clauses; when those
 *       queries are not applied, they return the actual values of the tested columns.
 *       <pre>
 * &#64;Delete(entityClass = Product.class, customIfClause = "description = :expectedDescription")
 * ResultSet deleteIfDescriptionMatches(UUID productId, String expectedDescription);
 * // if the condition fails, the result set will contain columns '[applied]' and 'description'
 *       </pre>
 *   <li>a {@link BoundStatement}. This is intended for queries where you will execute this
 *       statement later or in a batch.
 *       <pre>
 * &#64;Delete
 * BoundStatement delete(Product product);
 *       </pre>
 *   <li>a {@link CompletionStage} or {@link CompletableFuture} of any of the above. The method will
 *       execute the query asynchronously. Note that for result sets, you need to switch to {@link
 *       AsyncResultSet}.
 *       <pre>
 * &#64;Delete
 * CompletableFuture&lt;Void&gt; deleteAsync(Product product);
 *
 * &#64;Delete(ifExists = true)
 * CompletionStage&lt;Boolean&gt; deleteIfExistsAsync(Product product);
 *
 * &#64;Delete(entityClass = Product.class, customIfClause = "description = :expectedDescription")
 * CompletionStage&lt;AsyncResultSet&gt; deleteIfDescriptionMatchesAsync(UUID productId, String expectedDescription);
 *       </pre>
 *   <li>a {@link ReactiveResultSet}.
 *       <pre>
 * &#64;Delete
 * ReactiveResultSet deleteReactive(Product product);
 *       </pre>
 *   <li>a {@linkplain MapperResultProducer custom type}.
 * </ul>
 *
 * Note that you can also return a boolean or result set for non-conditional queries, but there's no
 * practical purpose for that since those queries always return {@code wasApplied = true} and an
 * empty result set.
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
public @interface Delete {

  /**
   * A hint to indicate the entity class, for cases where it can't be determined from the method's
   * signature.
   *
   * <p>This is only needed if the method receives the primary key components as arguments or uses a
   * custom where clause:
   *
   * <pre>
   * &#64;Delete(entityClass = Product.class)
   * void delete(UUID productId);
   *
   * &#64;Delete(entityClass = Product.class, customWhereClause="product_id = :productId")
   * void delete(UUID productId);
   * </pre>
   *
   * Note that, for technical reasons, this is an array, but only one element is expected. If you
   * specify more than one class, the mapper processor will generate a compile-time warning, and
   * proceed with the first one.
   */
  Class<?>[] entityClass() default {};

  /**
   * A custom WHERE clause for the DELETE query.
   *
   * <p>If this is not empty, it completely replaces the WHERE clause in the generated query. Note
   * that the provided string <b>must not</b> contain the {@code WHERE} keyword and {@link
   * #entityClass()} must be specified.
   *
   * <p>This clause can contain placeholders that will be bound with the method's parameters; see
   * the top-level javadocs of this class for more explanations.
   *
   * <p>Also note that this can be used in conjunction with {@link #customIfClause()} or {@link
   * #ifExists()}.
   */
  String customWhereClause() default "";

  /**
   * Whether to append an IF EXISTS clause at the end of the generated DELETE query.
   *
   * <p>This is mutually exclusive with {@link #customIfClause()} (if both are set, the mapper
   * processor will generate a compile-time error).
   */
  boolean ifExists() default false;

  /**
   * A custom IF clause for the DELETE query.
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
}
