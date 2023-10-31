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
package com.datastax.oss.driver.internal.querybuilder.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.schema.AlterMaterializedView;
import com.datastax.oss.driver.api.querybuilder.schema.AlterMaterializedViewStart;
import com.datastax.oss.driver.internal.querybuilder.CqlHelper;
import com.datastax.oss.driver.internal.querybuilder.ImmutableCollections;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultAlterMaterializedView
    implements AlterMaterializedViewStart, AlterMaterializedView {

  private final CqlIdentifier keyspace;
  private final CqlIdentifier viewName;

  private final ImmutableMap<String, Object> options;

  public DefaultAlterMaterializedView(@Nonnull CqlIdentifier viewName) {
    this(null, viewName);
  }

  public DefaultAlterMaterializedView(
      @Nullable CqlIdentifier keyspace, @Nonnull CqlIdentifier viewName) {
    this(keyspace, viewName, ImmutableMap.of());
  }

  public DefaultAlterMaterializedView(
      @Nullable CqlIdentifier keyspace,
      @Nonnull CqlIdentifier viewName,
      @Nonnull ImmutableMap<String, Object> options) {
    this.keyspace = keyspace;
    this.viewName = viewName;
    this.options = options;
  }

  @Nonnull
  @Override
  public AlterMaterializedView withOption(@Nonnull String name, @Nonnull Object value) {
    return new DefaultAlterMaterializedView(
        keyspace, viewName, ImmutableCollections.append(options, name, value));
  }

  @Nonnull
  @Override
  public String asCql() {
    StringBuilder builder = new StringBuilder("ALTER MATERIALIZED VIEW ");
    CqlHelper.qualify(keyspace, viewName, builder);
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

  @Nullable
  public CqlIdentifier getKeyspace() {
    return keyspace;
  }

  @Nonnull
  public CqlIdentifier getMaterializedView() {
    return viewName;
  }
}
