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
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * Annotates a {@link Dao} method that increments a counter table that is mapped to an {@link
 * Entity}-annotated class.
 *
 * <p>Example:
 *
 * <pre>
 * &#64;Entity
 * public class Votes {
 *   &#64;PartitionKey private int articleId;
 *   private long upVotes;
 *   private long downVotes;
 *   ... // constructor(s), getters and setters, etc.
 * }
 * &#64;Dao
 * public interface VotesDao {
 *   &#64;Increment(entityClass = Votes.class)
 *   void incrementUpVotes(int articleId, long upVotes);
 *
 *   &#64;Increment(entityClass = Votes.class)
 *   void incrementDownVotes(int articleId, long downVotes);
 *
 *   &#64;Select
 *   Votes findById(int articleId);
 * }
 * </pre>
 *
 * <h3>Parameters</h3>
 *
 * The entity class must be specified with {@link #entityClass()}.
 *
 * <p>The method's parameters must start with the full primary key, in the exact order (as defined
 * by the {@link PartitionKey} and {@link ClusteringColumn} annotations in the entity class). The
 * parameter names don't necessarily need to match the names of the columns, but the types must
 * match. Unlike other methods like {@link Select} or {@link Delete}, counter updates cannot operate
 * on a whole partition, they need to target exactly one row; so all the partition key and
 * clustering columns must be specified.
 *
 * <p>Then must follow one or more parameters representing counter increments. Their type must be
 * {@code long} or {@link Long}. The name of the parameter must match the name of the entity
 * property that maps to the counter (that is, the name of the getter without "get" and
 * decapitalized). Alternatively, you may annotate a parameter with {@link CqlName} to specify the
 * raw column name directly; in that case, the name of the parameter does not matter:
 *
 * <pre>
 * &#64;Increment(entityClass = Votes.class)
 * void incrementUpVotes(int articleId, &#64;CqlName("up_votes") long foobar);
 * </pre>
 *
 * When you invoke the method, each parameter value is interpreted as a <b>delta</b> that will be
 * applied to the counter. In other words, if you pass 1, the counter will be incremented by 1.
 * Negative values are allowed. If you are using Cassandra 2.2 or above, you can use {@link Long}
 * and pass {@code null} for some of the parameters, they will be ignored (following {@link
 * NullSavingStrategy#DO_NOT_SET} semantics). If you are using Cassandra 2.1, {@code null} values
 * will trigger a runtime error.
 *
 * <p>A {@link Function Function&lt;BoundStatementBuilder, BoundStatementBuilder&gt;} or {@link
 * UnaryOperator UnaryOperator&lt;BoundStatementBuilder&gt;} can be added as the <b>last</b>
 * parameter. It will be applied to the statement before execution. This allows you to customize
 * certain aspects of the request (page size, timeout, etc) at runtime.
 *
 * <h3>Return type</h3>
 *
 * <p>The method can return {@code void}, a void {@link CompletionStage} or {@link
 * CompletableFuture}, or a {@link ReactiveResultSet}.
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
public @interface Increment {

  /**
   * A hint to indicate the entity class that is being targeted. This is mandatory, the mapper will
   * issue a compile error if you leave it unset.
   *
   * <p>Note that, for technical reasons, this is an array, but only one element is expected. If you
   * specify more than one class, the mapper processor will generate a compile-time warning, and
   * proceed with the first one.
   */
  Class<?>[] entityClass() default {};
}
