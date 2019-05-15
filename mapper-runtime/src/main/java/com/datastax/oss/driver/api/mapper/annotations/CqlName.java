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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates an {@link Entity} class or one of its properties (field or getter), to specify a custom
 * CQL name.
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
 * </pre>
 *
 * This annotation takes precedence over the {@link NamingStrategy naming strategy} defined for the
 * entity.
 */
@Target({ElementType.TYPE, ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.CLASS)
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
