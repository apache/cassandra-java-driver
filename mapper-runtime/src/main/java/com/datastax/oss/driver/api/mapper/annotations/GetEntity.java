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

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.GettableByName;
import com.datastax.oss.driver.api.core.data.UdtValue;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates a {@link Dao} method that converts a core driver data structure into one or more
 * instances of an {@link Entity} class.
 *
 * <p>Example:
 *
 * <pre>
 * &#64;Dao
 * public interface ProductDao {
 *   &#64;GetEntity
 *   Product asProduct(Row row);
 * }
 * </pre>
 *
 * The generated code will retrieve each entity property from the source, such as:
 *
 * <pre>
 * Product product = new Product();
 * product.setId(row.get("id", UUID.class));
 * product.setDescription(row.get("description", String.class));
 * ...
 * </pre>
 *
 * <p>It does not perform a query. Instead, those methods are intended for cases where you already
 * have a query result, and just need the conversion logic.
 *
 * <h3>Parameters</h3>
 *
 * The method must have a single parameter. The following types are allowed:
 *
 * <ul>
 *   <li>{@link GettableByName} or one of its subtypes (the most likely candidates are {@link Row}
 *       and {@link UdtValue}).
 *   <li>{@link ResultSet}.
 *   <li>{@link AsyncResultSet}.
 * </ul>
 *
 * The data must match the target entity: the generated code will try to extract every mapped
 * property, and fail if one is missing.
 *
 * <h3>Return type</h3>
 *
 * The method can return:
 *
 * <ul>
 *   <li>a single entity instance. If the argument is a result set type, the generated code will
 *       extract the first row and convert it, or return {@code null} if the result set is empty.
 *       <pre>
 * &#64;GetEntity
 * Product asProduct(Row row);
 *
 * &#64;GetEntity
 * Product firstRowAsProduct(ResultSet resultSet);
 *       </pre>
 *   <li>a {@link PagingIterable} of an entity class. In that case, the type of the parameter
 *       <b>must</b> be {@link ResultSet}. Each row in the result set will be converted into an
 *       entity instance.
 *       <pre>
 * &#64;GetEntity
 * PagingIterable&lt;Product&gt; asProducts(ResultSet resultSet);
 *       </pre>
 *   <li>a {@link MappedAsyncPagingIterable} of an entity class. In that case, the type of the
 *       parameter <b>must</b> be {@link AsyncResultSet}. Each row in the result set will be
 *       converted into an entity instance.
 *       <pre>
 * &#64;GetEntity
 * MappedAsyncPagingIterable&lt;Product&gt; asProducts(AsyncResultSet resultSet);
 *       </pre>
 * </ul>
 *
 * If the return type doesn't match the parameter type (for example {@link PagingIterable} for
 * {@link AsyncResultSet}), the mapper processor will issue a compile-time error.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface GetEntity {

  /**
   * Whether to tolerate missing columns in the source data structure.
   *
   * <p>If {@code false} (the default), then the source must contain a matching column for every
   * property in the entity definition, <em>including computed ones</em>. If such a column is not
   * found, an {@link IllegalArgumentException} will be thrown.
   *
   * <p>If {@code true}, the mapper will operate on a best-effort basis and attempt to read all
   * entity properties that have a matching column in the source, leaving unmatched properties
   * untouched. Beware that this may result in a partially-populated entity instance.
   */
  boolean lenient() default false;
}
