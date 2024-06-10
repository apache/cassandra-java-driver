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
import com.datastax.dse.driver.api.mapper.reactive.MappedReactiveResultSet;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
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
 * Annotates a {@link Dao} method that executes a user-provided query.
 *
 * <p>Example:
 *
 * <pre>
 * &#64;Dao
 * public interface SensorReadingDao {
 *   &#64;Query("SELECT count(*) FROM sensor_readings WHERE id = :id")
 *   long countById(int id);
 * }
 * </pre>
 *
 * This is the equivalent of what was called "accessor methods" in the driver 3 mapper.
 *
 * <h3>Parameters</h3>
 *
 * The query string provided in {@link #value()} will typically contain CQL placeholders. The
 * method's parameters must match those placeholders: same name and a compatible Java type.
 *
 * <pre>
 * &#64;Query("SELECT count(*) FROM sensor_readings WHERE id = :id AND year = :year")
 * long countByIdAndYear(int id, int year);
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
 *       ResultSet#wasApplied()}. This is intended for conditional queries.
 *   <li>a {@code long} or {@link Long}, which will be mapped to the first column of the first row,
 *       expecting CQL type {@code BIGINT}. This is intended for count queries. The method will fail
 *       if the result set is empty, or does not match the expected format.
 *   <li>a {@link Row}. This means the result is not converted, the mapper only extracts the first
 *       row of the result set and returns it. The method will return {@code null} if the result set
 *       is empty.
 *   <li>a single instance of an {@link Entity} class. The method will extract the first row and
 *       convert it, or return {@code null} if the result set is empty.
 *   <li>an {@link Optional} of an entity class. The method will extract the first row and convert
 *       it, or return {@code Optional.empty()} if the result set is empty.
 *   <li>a {@link ResultSet}. The method will return the raw query result, without any conversion.
 *   <li>a {@link BoundStatement}. This is intended for cases where you intend to execute this
 *       statement later or in a batch:
 *   <li>a {@link PagingIterable}. The method will convert each row into an entity instance.
 *   <li>a {@link CompletionStage} or {@link CompletableFuture} of any of the above. The method will
 *       execute the query asynchronously. Note that for result sets and iterables, you need to
 *       switch to the asynchronous equivalent {@link AsyncResultSet} and {@link
 *       MappedAsyncPagingIterable} respectively.
 *   <li>a {@link ReactiveResultSet}, or a {@link MappedReactiveResultSet} of the entity class.
 *   <li>a {@linkplain MapperResultProducer custom type}.
 * </ul>
 *
 * <h3>Target keyspace and table</h3>
 *
 * To avoid hard-coding the keyspace and table name, the query string supports 3 additional
 * placeholders: {@code ${keyspaceId}}, {@code ${tableId}} and {@code ${qualifiedTableId}}. They get
 * substituted at DAO initialization time, with the keyspace and table that the DAO was built with
 * (see {@link DaoFactory}).
 *
 * <p>For example, given the following:
 *
 * <pre>
 * &#64;Dao
 * public interface TestDao {
 *   &#64;Query("SELECT * FROM ${keyspaceId}.${tableId}")
 *   ResultSet queryFromKeyspaceAndTable();
 *
 *   &#64;Query("SELECT * FROM ${qualifiedTableId}")
 *   ResultSet queryFromQualifiedTable();
 * }
 *
 * &#64;Mapper
 * public interface TestMapper {
 *   &#64;DaoFactory
 *   TestDao dao(&#64;DaoKeyspace String keyspace, &#64;DaoTable String table);
 *
 *   &#64;DaoFactory
 *   TestDao dao(&#64;DaoTable String table);
 * }
 *
 * TestDao dao1 = mapper.dao("ks", "t");
 * TestDao dao2 = mapper.dao("t");
 * </pre>
 *
 * Then:
 *
 * <ul>
 *   <li>{@code dao1.queryFromKeyspaceAndTable()} and {@code dao1.queryFromQualifiedTable()} both
 *       execute {@code SELECT * FROM ks.t}.
 *   <li>{@code dao2.queryFromKeyspaceAndTable()} fails: no keyspace was specified for this DAO, so
 *       {@code ${keyspaceId}} can't be substituted.
 *   <li>{@code dao1.queryFromQualifiedTable()} executes {@code SELECT * FROM t}. In other words,
 *       {@code ${qualifiedTableId}} uses the keyspace if it is available, but resolves to the table
 *       name only if it isn't. Whether the query succeeds or not depends on whether the {@link
 *       Session} that the mapper was built with has a {@linkplain
 *       SessionBuilder#withKeyspace(CqlIdentifier) default keyspace}.
 * </ul>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Query {

  /**
   * The query string to execute.
   *
   * <p>It can contain CQL placeholders (e.g. {@code :id}) that will be bound with the method's
   * parameters; and also special text placeholders {@code ${keyspaceId}}, {@code ${tableId}} and
   * {@code ${qualifiedTableId}} that will be substituted with the keyspace and table that the DAO
   * was built with. See the top-level javadocs of this class for more explanations.
   */
  String value();

  /**
   * How to handle null query parameters.
   *
   * <p>This defaults either to the {@link DefaultNullSavingStrategy DAO-level strategy} (if set),
   * or {@link NullSavingStrategy#DO_NOT_SET}.
   */
  NullSavingStrategy nullSavingStrategy() default NullSavingStrategy.DO_NOT_SET;
}
