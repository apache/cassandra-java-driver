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
package com.datastax.dse.driver.internal.core.metadata.schema;

import com.datastax.dse.driver.api.core.metadata.schema.DseGraphKeyspaceMetadata;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.AggregateMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionSignature;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultDseKeyspaceMetadata implements DseGraphKeyspaceMetadata, Serializable {

  private static final long serialVersionUID = 1;

  @Nonnull private final CqlIdentifier name;
  private final boolean durableWrites;
  private final boolean virtual;
  @Nullable private final String graphEngine;
  @Nonnull private final Map<String, String> replication;
  @Nonnull private final Map<CqlIdentifier, UserDefinedType> types;
  @Nonnull private final Map<CqlIdentifier, TableMetadata> tables;
  @Nonnull private final Map<CqlIdentifier, ViewMetadata> views;
  @Nonnull private final Map<FunctionSignature, FunctionMetadata> functions;
  @Nonnull private final Map<FunctionSignature, AggregateMetadata> aggregates;

  public DefaultDseKeyspaceMetadata(
      @Nonnull CqlIdentifier name,
      boolean durableWrites,
      boolean virtual,
      @Nullable String graphEngine,
      @Nonnull Map<String, String> replication,
      @Nonnull Map<CqlIdentifier, UserDefinedType> types,
      @Nonnull Map<CqlIdentifier, TableMetadata> tables,
      @Nonnull Map<CqlIdentifier, ViewMetadata> views,
      @Nonnull Map<FunctionSignature, FunctionMetadata> functions,
      @Nonnull Map<FunctionSignature, AggregateMetadata> aggregates) {
    this.name = name;
    this.durableWrites = durableWrites;
    this.virtual = virtual;
    this.graphEngine = graphEngine;
    this.replication = replication;
    this.types = types;
    this.tables = tables;
    this.views = views;
    this.functions = functions;
    this.aggregates = aggregates;
  }

  @Nonnull
  @Override
  public CqlIdentifier getName() {
    return name;
  }

  @Override
  public boolean isDurableWrites() {
    return durableWrites;
  }

  @Override
  public boolean isVirtual() {
    return virtual;
  }

  @Nonnull
  @Override
  public Optional<String> getGraphEngine() {
    return Optional.ofNullable(graphEngine);
  }

  @Nonnull
  @Override
  public Map<String, String> getReplication() {
    return replication;
  }

  @Nonnull
  @Override
  public Map<CqlIdentifier, UserDefinedType> getUserDefinedTypes() {
    return types;
  }

  @Nonnull
  @Override
  public Map<CqlIdentifier, TableMetadata> getTables() {
    return tables;
  }

  @Nonnull
  @Override
  public Map<CqlIdentifier, ViewMetadata> getViews() {
    return views;
  }

  @Nonnull
  @Override
  public Map<FunctionSignature, FunctionMetadata> getFunctions() {
    return functions;
  }

  @Nonnull
  @Override
  public Map<FunctionSignature, AggregateMetadata> getAggregates() {
    return aggregates;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof DseGraphKeyspaceMetadata) {
      DseGraphKeyspaceMetadata that = (DseGraphKeyspaceMetadata) other;
      return Objects.equals(this.name, that.getName())
          && this.durableWrites == that.isDurableWrites()
          && this.virtual == that.isVirtual()
          && Objects.equals(this.graphEngine, that.getGraphEngine().orElse(null))
          && Objects.equals(this.replication, that.getReplication())
          && Objects.equals(this.types, that.getUserDefinedTypes())
          && Objects.equals(this.tables, that.getTables())
          && Objects.equals(this.views, that.getViews())
          && Objects.equals(this.functions, that.getFunctions())
          && Objects.equals(this.aggregates, that.getAggregates());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        name,
        durableWrites,
        virtual,
        graphEngine,
        replication,
        types,
        tables,
        views,
        functions,
        aggregates);
  }

  @Override
  public boolean shallowEquals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof DseGraphKeyspaceMetadata) {
      DseGraphKeyspaceMetadata that = (DseGraphKeyspaceMetadata) other;
      return Objects.equals(this.name, that.getName())
          && this.durableWrites == that.isDurableWrites()
          && Objects.equals(this.graphEngine, that.getGraphEngine().orElse(null))
          && Objects.equals(this.replication, that.getReplication());
    } else {
      return false;
    }
  }
}
