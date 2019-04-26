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
package com.datastax.oss.driver.mapper.model.inventory;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.GetEntity;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.annotations.SetEntity;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

@Dao
public interface ProductDao {

  @SetEntity
  BoundStatement set(Product product, BoundStatement boundStatement);

  @SetEntity
  void set(BoundStatementBuilder builder, Product product);

  @SetEntity
  void set(Dimensions dimensions, UdtValue udtValue);

  @GetEntity
  Product get(Row row);

  @GetEntity
  PagingIterable<Product> get(ResultSet resultSet);

  @GetEntity
  MappedAsyncPagingIterable<Product> get(AsyncResultSet resultSet);

  @GetEntity
  Product getOne(ResultSet resultSet);

  @GetEntity
  Product getOne(AsyncResultSet resultSet);

  @Insert
  void save(Product product);

  @Insert(customClause = "USING TIMESTAMP :timestamp")
  Product saveWithBoundTimestamp(Product product, long timestamp);

  @Select
  Product findById(UUID productId);

  @Select
  CompletionStage<Product> findByIdAsync(UUID productId);

  /** Note that this relies on a SASI index. */
  @Select(customWhereClause = "WHERE description LIKE :searchString")
  PagingIterable<Product> findByDescription(String searchString);

  /** Note that this relies on a SASI index. */
  @Select(customWhereClause = "WHERE description LIKE :searchString")
  CompletionStage<MappedAsyncPagingIterable<Product>> findByDescriptionAsync(String searchString);
}
