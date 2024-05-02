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
package com.datastax.oss.driver.internal.mapper.processor.mapper;

import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.squareup.javapoet.TypeName;

/**
 * Exposes callbacks that allow individual method generators for a {@link Mapper}-annotated class to
 * request the generation of class-level fields that they will use.
 */
public interface MapperImplementationSharedCode {

  /**
   * Requests the generation of a field that caches a single DAO instance, initialized in the
   * constructor.
   *
   * <p>For example:
   *
   * <pre>{@code
   * private final LazyReference<ProductDao> productDaoCache;
   *
   * public MyMapper_Impl(MapperContext context) {
   *   this.productDaoCache = new LazyReference<>(() -> ProductDao_Impl.init(context));
   * }
   * }</pre>
   *
   * @return the name of the new field: {@code suggestedFieldName}, possibly prefixed by an index to
   *     avoid duplicates.
   */
  String addDaoSimpleField(
      String suggestedFieldName,
      TypeName fieldType,
      TypeName daoImplementationType,
      boolean isAsync);

  /**
   * Requests the generation of a map field, for DAOs that can be fetched by keyspace and/or table.
   *
   * <p>For example:
   *
   * <pre>{@code
   * private final ConcurrentMap<DaoCacheKey, ProductDao> productDaoCache = new ConcurrentHashMap<>();
   * }</pre>
   *
   * @return the name of the new field: {@code suggestedFieldName}, possibly prefixed by an index to
   *     avoid duplicates.
   */
  String addDaoMapField(String suggestedFieldName, TypeName mapValueType);
}
