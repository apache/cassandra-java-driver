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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
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
 * Annotates a {@link Dao} method that selects one or more rows, and maps them to instances of an
 * {@link Entity}-annotated class.
 *
 * <p>Example:
 *
 * <pre>
 * &#64;Dao
 * public interface ProductDao {
 *   &#64;Select
 *   Product findById(UUID productId);
 * }
 * </pre>
 *
 * <h3>Parameters</h3>
 *
 * If {@link #customWhereClause()} is empty, the mapper defaults to a selection by primary key
 * (partition key + clustering columns). The method's parameters must match the types of the primary
 * key columns, in the exact order (which is defined by the integer values of the {@link
 * PartitionKey} and {@link ClusteringColumn} annotations in the entity class). The parameter names
 * don't necessarily need to match the names of the columns.
 *
 * <p>If {@link #customWhereClause()} is not empty, it completely replaces the WHERE clause. The
 * provided string can contain named placeholders. In that case, the method must have a
 * corresponding parameter for each, with the same name and a compatible Java type:
 *
 * <pre>
 * &#64;Select(customWhereClause = "description LIKE :searchString")
 * PagingIterable&lt;Product&gt; findByDescription(String searchString);
 * </pre>
 *
 * <p>A {@link Function Function&lt;BoundStatementBuilder, BoundStatementBuilder&gt;} or {@link
 * UnaryOperator UnaryOperator&lt;BoundStatementBuilder&gt;} can be added as the <b>last</b>
 * parameter. It will be applied to the statement before execution. This allows you to customize
 * certain aspects of the request (page size, timeout, etc) at runtime.
 *
 * <h3>Return type</h3>
 *
 * <p>In all cases, the method can return:
 *
 * <ul>
 *   <li>the entity class itself. If the query returns no rows, the method will return {@code null}.
 *       If it returns more than one row, subsequent rows will be discarded.
 *       <pre>
 * &#64;Select
 * Product findById(UUID productId);
 *       </pre>
 *   <li>an {@link Optional} of the entity class. If the query returns no rows, the method will
 *       return {@code Optional.empty()}. If it returns more than one row, subsequent rows will be
 *       discarded.
 *       <pre>
 * &#64;Select
 * Optional&lt;Product&gt; findById(UUID productId);
 *       </pre>
 *   <li>a {@link PagingIterable} of the entity class. It behaves like a result set, except that
 *       each element is a mapped entity instead of a row.
 *       <pre>
 * &#64;Select(customWhereClause = "description LIKE :searchString")
 * PagingIterable&lt;Product&gt; findByDescription(String searchString);
 *       </pre>
 *   <li>a {@link CompletionStage} or {@link CompletableFuture} of any of the above. The method will
 *       execute the query asynchronously. Note that for iterables, you need to switch to the
 *       asynchronous equivalent {@link MappedAsyncPagingIterable}.
 *       <pre>
 * &#64;Select
 * CompletionStage&lt;Product&gt; findByIdAsync(UUID productId);
 *
 * &#64;Select
 * CompletionStage&lt;Optional&lt;Product&gt;&gt; findByIdAsync(UUID productId);
 *
 * &#64;Select(customWhereClause = "description LIKE :searchString")
 * CompletionStage&lt;MappedAsyncPagingIterable&lt;Product&gt;&gt; findByDescriptionAsync(String searchString);
 *       </pre>
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
@Retention(RetentionPolicy.CLASS)
public @interface Select {

  /**
   * A custom WHERE clause for the SELECT query.
   *
   * <p>If this is not empty, it completely replaces the WHERE clause in the generated query. Note
   * that the provided string <b>must not</b> contain the {@code WHERE} keyword.
   *
   * <p>This clause can contain placeholders that will be bound with the method's parameters; see
   * the top-level javadocs of this class for more explanations.
   */
  String customWhereClause() default "";
}
