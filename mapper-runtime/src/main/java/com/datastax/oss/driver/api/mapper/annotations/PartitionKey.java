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
 * Annotates the field or getter of an {@link Entity} property, to indicate that it's part of the
 * partition key.
 *
 * <p>Example:
 *
 * <pre>
 * &#64;PartitionKey private UUID id;
 * </pre>
 *
 * This information is used by the mapper processor to generate default queries (for example a basic
 * {@link Select}). Note that an entity is not required to have a partition key, for example if it
 * only gets mapped as a UDT.
 *
 * <p>If the partition key is composite, you must specify {@link #value()} to indicate the position
 * of each property:
 *
 * <pre>
 * &#64;PartitionKey(1) private int pk1;
 * &#64;PartitionKey(2) private int pk2;
 * </pre>
 *
 * If you don't specify positions, or if there are duplicates, the mapper processor will issue a
 * compile-time error.
 *
 * <p>This annotation is mutually exclusive with {@link ClusteringColumn}.
 */
@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface PartitionKey {

  /**
   * The position of the property in the partition key.
   *
   * <p>This is only required if the partition key is composite. Positions are not strictly required
   * to be consecutive or start at a given index, but for clarity it is recommended to use
   * consecutive integers.
   */
  int value() default 0;
}
