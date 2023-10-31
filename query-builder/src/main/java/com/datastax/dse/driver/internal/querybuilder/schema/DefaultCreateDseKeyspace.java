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
package com.datastax.dse.driver.internal.querybuilder.schema;

import com.datastax.dse.driver.api.querybuilder.schema.CreateDseKeyspace;
import com.datastax.dse.driver.api.querybuilder.schema.CreateDseKeyspaceStart;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.internal.querybuilder.ImmutableCollections;
import com.datastax.oss.driver.internal.querybuilder.schema.OptionsUtils;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.Map;
import javax.annotation.Nonnull;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultCreateDseKeyspace implements CreateDseKeyspace, CreateDseKeyspaceStart {

  private final CqlIdentifier keyspaceName;
  private final boolean ifNotExists;
  private final ImmutableMap<String, Object> options;

  public DefaultCreateDseKeyspace(@Nonnull CqlIdentifier keyspaceName) {
    this(keyspaceName, false, ImmutableMap.of());
  }

  public DefaultCreateDseKeyspace(
      @Nonnull CqlIdentifier keyspaceName,
      boolean ifNotExists,
      @Nonnull ImmutableMap<String, Object> options) {
    this.keyspaceName = keyspaceName;
    this.ifNotExists = ifNotExists;
    this.options = options;
  }

  @Nonnull
  @Override
  public CreateDseKeyspace withOption(@Nonnull String name, @Nonnull Object value) {
    return new DefaultCreateDseKeyspace(
        keyspaceName, ifNotExists, ImmutableCollections.append(options, name, value));
  }

  @Nonnull
  @Override
  public CreateDseKeyspaceStart ifNotExists() {
    return new DefaultCreateDseKeyspace(keyspaceName, true, options);
  }

  @Nonnull
  @Override
  public CreateDseKeyspace withReplicationOptions(@Nonnull Map<String, Object> replicationOptions) {
    return withOption("replication", replicationOptions);
  }

  @Nonnull
  @Override
  public String asCql() {
    StringBuilder builder = new StringBuilder();

    builder.append("CREATE KEYSPACE ");
    if (ifNotExists) {
      builder.append("IF NOT EXISTS ");
    }

    builder.append(keyspaceName.asCql(true));
    builder.append(OptionsUtils.buildOptions(options, true));
    return builder.toString();
  }

  @Override
  public String toString() {
    return asCql();
  }

  @Nonnull
  @Override
  public Map<String, Object> getOptions() {
    return options;
  }

  @Nonnull
  public CqlIdentifier getKeyspace() {
    return keyspaceName;
  }

  public boolean isIfNotExists() {
    return ifNotExists;
  }
}
