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
import java.util.List;
import java.util.Optional;

public class DefaultEntityDefinition implements EntityDefinition {

  private final ClassName className;
  private final CodeBlock cqlName;
  private final List<PropertyDefinition> partitionKey;
  private final List<PropertyDefinition> clusteringColumns;
  private final ImmutableList<PropertyDefinition> regularColumns;

  public DefaultEntityDefinition(
      ClassName className,
      String javaName,
      Optional<String> customCqlName,
      List<PropertyDefinition> partitionKey,
      List<PropertyDefinition> clusteringColumns,
      List<PropertyDefinition> regularColumns,
      CqlNameGenerator cqlNameGenerator) {
    this.className = className;
    this.cqlName =
        customCqlName
            .map(n -> CodeBlock.of("$S", n))
            .orElse(cqlNameGenerator.buildCqlName(javaName));
    this.partitionKey = partitionKey;
    this.clusteringColumns = clusteringColumns;
    this.regularColumns = ImmutableList.copyOf(regularColumns);
  }

  @Override
  public ClassName getClassName() {
    return className;
  }

  @Override
  public CodeBlock getCqlName() {
    return cqlName;
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
}
