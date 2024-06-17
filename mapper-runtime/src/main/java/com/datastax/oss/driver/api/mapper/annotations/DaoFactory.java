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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates a DAO-producing method in a {@link Mapper} interface.
 *
 * <p>Example:
 *
 * <pre>
 * &#64;Mapper
 * public interface InventoryMapper {
 *   &#64;DaoFactory
 *   ProductDao productDao();
 * }
 * </pre>
 *
 * The return type of the method must be a {@link Dao}-annotated interface.
 *
 * <p>If the method takes no arguments, the DAO operates on the session's default keyspace (assuming
 * that one was set), and the entity's default table:
 *
 * <pre>
 * // Example 1: the session has a default keyspace
 * CqlSession session = CqlSession.builder().withKeyspace("test").build();
 * InventoryMapper inventoryMapper = new InventoryMapperBuilder(session).build();
 * ProductDao dao = inventoryMapper.productDao();
 * Product product = dao.selectById(1);
 * // =&gt; success (selects from test.product)
 *
 * // Example 2: the session has no default keyspace
 * CqlSession session = CqlSession.builder().build();
 * InventoryMapper inventoryMapper = new InventoryMapperBuilder(session).build();
 * ProductDao dao = inventoryMapper.productDao();
 * Product product = dao.selectById(1);
 * // =&gt; CQL error (No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename)
 * </pre>
 *
 * You can also have the method take the keyspace and table as arguments (annotated respectively
 * with {@link DaoKeyspace} and {@link DaoTable}):
 *
 * <pre>
 * &#64;Mapper
 * public interface InventoryMapper {
 *   &#64;DaoFactory
 *   ProductDao productDao(@DaoKeyspace String keyspace);
 *
 *   &#64;DaoFactory
 *   ProductDao productDao(@DaoKeyspace String keyspace, @DaoTable String table);
 * }
 * </pre>
 *
 * This allows you to reuse the same DAO interface to operate on different tables:
 *
 * <pre>
 * ProductDao dao1 = inventoryMapper.productDao("keyspace1");
 * Product product = dao1.selectById(1); // selects from keyspace1.product
 *
 * ProductDao dao2 = inventoryMapper.productDao("keyspace2");
 * Product product = dao2.selectById(1); // selects from keyspace2.product
 *
 * ProductDao dao3 = inventoryMapper.productDao("keyspace3", "table3");
 * Product product = dao3.selectById(1); // selects from keyspace3.table3
 * </pre>
 *
 * In all cases, DAO instances are initialized lazily and cached for future calls:
 *
 * <pre>
 * ProductDao dao1 = inventoryMapper.productDao("keyspace1", "product");
 * ProductDao dao2 = inventoryMapper.productDao("keyspace1", "product");
 * assert dao1 == dao2; // same arguments, same instance
 * </pre>
 *
 * Note that the cache is a simple map with no eviction mechanism.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface DaoFactory {}
