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

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.List;
import java.util.Optional;

public class DefaultEntityDefinition implements EntityDefinition {

  private final ClassName className;
  private final CodeBlock cqlName;
  private final List<PropertyDefinition> partitionKey;
  private final List<PropertyDefinition> clusteringColumns;
  private final ImmutableList<PropertyDefinition> regularColumns;
  private final ImmutableList<PropertyDefinition> computedValues;
  private final String defaultKeyspace;
  private final boolean mutable;

  public DefaultEntityDefinition(
      ClassName className,
      String javaName,
      String defaultKeyspace,
      Optional<String> customCqlName,
      List<PropertyDefinition> partitionKey,
      List<PropertyDefinition> clusteringColumns,
      List<PropertyDefinition> regularColumns,
      List<PropertyDefinition> computedValues,
      CqlNameGenerator cqlNameGenerator,
      boolean mutable) {
    this.className = className;
    this.cqlName =
        customCqlName
            .map(n -> CodeBlock.of("$S", n))
            .orElse(cqlNameGenerator.buildCqlName(javaName));
    this.defaultKeyspace = defaultKeyspace;
    this.partitionKey = partitionKey;
    this.clusteringColumns = clusteringColumns;
    this.regularColumns = ImmutableList.copyOf(regularColumns);
    this.computedValues = ImmutableList.copyOf(computedValues);
    this.mutable = mutable;
  }

  @Override
  public ClassName getClassName() {
    return className;
  }

  @Override
  public CodeBlock getCqlName() {
    return cqlName;
  }

  @Nullable
  @Override
  public String getDefaultKeyspace() {
    return defaultKeyspace;
  }

  @Override
  public List<PropertyDefinition> getPartitionKey() {
    return partitionKey;
  }

  @Override
  public List<PropertyDefinition> getClusteringColumns() {
    return clusteringColumns;
  }

  @Override
  public Iterable<PropertyDefinition> getRegularColumns() {
    return regularColumns;
  }

  @Override
  public Iterable<PropertyDefinition> getComputedValues() {
    return computedValues;
  }

  @Override
  public boolean isMutable() {
    return mutable;
  }
}
