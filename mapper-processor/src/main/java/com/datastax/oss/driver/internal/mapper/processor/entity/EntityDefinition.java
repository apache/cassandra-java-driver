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
package com.datastax.oss.driver.internal.mapper.processor.entity;

import com.datastax.oss.driver.api.mapper.annotations.PropertyStrategy;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.List;

public interface EntityDefinition {

  ClassName getClassName();

  CodeBlock getCqlName();

  @Nullable
  String getDefaultKeyspace();

  List<PropertyDefinition> getPartitionKey();

  List<PropertyDefinition> getClusteringColumns();

  /**
   * @return the primary key, obtained by concatenating {@link #getPartitionKey()} and {@link
   *     #getClusteringColumns()}, in that order.
   */
  default List<PropertyDefinition> getPrimaryKey() {
    return ImmutableList.<PropertyDefinition>builder()
        .addAll(getPartitionKey())
        .addAll(getClusteringColumns())
        .build();
  }

  Iterable<PropertyDefinition> getRegularColumns();

  Iterable<PropertyDefinition> getComputedValues();

  /**
   * @return the concatenation of {@link #getPartitionKey()}, {@link #getClusteringColumns()} and
   *     {@link #getRegularColumns()}, in that order.
   */
  default List<PropertyDefinition> getAllColumns() {
    return ImmutableList.<PropertyDefinition>builder()
        .addAll(getPartitionKey())
        .addAll(getClusteringColumns())
        .addAll(getRegularColumns())
        .build();
  }

  /**
   * @return the concatenation of {@link #getPartitionKey()}, {@link #getClusteringColumns()},
   *     {@link #getRegularColumns()}, and {@link #getComputedValues()} in that order.
   */
  default List<PropertyDefinition> getAllValues() {
    return ImmutableList.<PropertyDefinition>builder()
        .addAll(getPartitionKey())
        .addAll(getClusteringColumns())
        .addAll(getRegularColumns())
        .addAll(getComputedValues())
        .build();
  }

  /** @see PropertyStrategy#mutable() */
  boolean isMutable();
}
