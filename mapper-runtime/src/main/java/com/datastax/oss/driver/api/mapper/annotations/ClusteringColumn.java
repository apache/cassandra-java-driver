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
 * Annotates the field or getter of an {@link Entity} property, to indicate that it's a clustering
 * column.
 *
 * <p>Example:
 *
 * <pre>
 * &#64;ClusteringColumn private int month;
 * </pre>
 *
 * This information is used by the mapper processor to generate default queries (for example a basic
 * {@link Select}).
 *
 * <p>If there are multiple clustering columns, you must specify {@link #value()} to indicate the
 * position of each property:
 *
 * <pre>
 * &#64;ClusteringColumn(1) private int month;
 * &#64;ClusteringColumn(2) private int day;
 * </pre>
 *
 * If you don't specify positions, or if there are duplicates, the mapper processor will issue a
 * compile-time error.
 *
 * <p>This annotation is mutually exclusive with {@link PartitionKey}.
 */
@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface ClusteringColumn {

  /**
   * The position of the clustering column.
   *
   * <p>This is only required if there are multiple clustering columns. Positions are not strictly
   * required to be consecutive or start at a given index, but for clarity it is recommended to use
   * consecutive integers.
   */
  int value() default 0;
}
