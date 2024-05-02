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
 * Annotates an {@link Entity} to indicate which properties should be considered 'transient',
 * meaning that they should not be mapped to any column (neither during reads nor writes).
 *
 * <p>Example:
 *
 * <pre>
 * &#64;TransientProperties({"notAColumn", "x"})
 * &#64;Entity
 * public class Product {
 *   &#64;PartitionKey private UUID id;
 *   private String description;
 *   // these columns are not included because their names are specified in &#64;TransientProperties
 *   private int notAColumn;
 *   private int x;
 *   ...
 * }
 * </pre>
 *
 * <p>This annotation is an alternative to using the {@link Transient} annotation on a field or
 * getter. It is useful in cases where this annotation may be specified on a parent class where
 * implementing classes will share a common configuration without needing to explicitly annotate
 * each property with a {@link Transient} annotation.
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface TransientProperties {

  /** Specifies a list of property names that should be considered transient. */
  String[] value() default {};
}
