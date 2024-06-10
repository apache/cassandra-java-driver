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
 * Annotates an {@link Entity} class or one of its properties (field or getter), to specify a custom
 * CQL name.
 *
 * <p>This annotation can also be used to annotate DAO method parameters when they correspond to
 * bind markers in custom {@code WHERE} clauses; if the parameter is not annotated with {@link
 * CqlName}, its programmatic name will be used instead as the bind marker name.
 *
 * <p>Beware that if the DAO interface was pre-compiled and is located in a jar, then all its bind
 * marker parameters must be annotated with {@link CqlName}; failing to do so will result in a
 * compilation failure because class files do not retain parameter names by default. If you intend
 * to distribute your DAO interface in a pre-compiled fashion, it is preferable to always annotate
 * such method parameters.
 *
 * <p>Example:
 *
 * <pre>
 * &#64;Entity
 * public class Product {
 *   &#64;PartitionKey
 *   &#64;CqlName("product_id")
 *   private int id;
 *   ...
 * }
 * &#64;Dao
 * public class ProductDao {
 *     &#64;Query("SELECT count(*) FROM ${qualifiedTableId} WHERE product_id = :product_id")
 *     long countById(@CqlName("product_id") int id);
 *   ...
 * }
 * </pre>
 *
 * This annotation takes precedence over the {@link NamingStrategy naming strategy} defined for the
 * entity.
 */
@Target({ElementType.TYPE, ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface CqlName {

  /**
   * The CQL name to use for this entity or property.
   *
   * <p>If you want it to be case-sensitive, it must be enclosed in double-quotes, for example:
   *
   * <pre>
   * &#64;CqlName("\"productId\"")
   * private int id;
   * </pre>
   */
  String value();
}
