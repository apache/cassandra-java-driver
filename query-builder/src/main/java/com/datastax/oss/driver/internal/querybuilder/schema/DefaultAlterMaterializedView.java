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
package com.datastax.oss.driver.internal.querybuilder.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.schema.AlterMaterializedView;
import com.datastax.oss.driver.api.querybuilder.schema.AlterMaterializedViewStart;
import com.datastax.oss.driver.internal.querybuilder.CqlHelper;
import com.datastax.oss.driver.internal.querybuilder.ImmutableCollections;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

public class DefaultAlterMaterializedView
    implements AlterMaterializedViewStart, AlterMaterializedView {

  private final CqlIdentifier keyspace;
  private final CqlIdentifier viewName;

  private final ImmutableMap<String, Object> options;

  public DefaultAlterMaterializedView(CqlIdentifier viewName) {
    this(null, viewName);
  }

  public DefaultAlterMaterializedView(CqlIdentifier keyspace, CqlIdentifier viewName) {
    this(keyspace, viewName, ImmutableMap.of());
  }

  public DefaultAlterMaterializedView(
      CqlIdentifier keyspace, CqlIdentifier viewName, ImmutableMap<String, Object> options) {
    this.keyspace = keyspace;
    this.viewName = viewName;
    this.options = options;
  }

  @Override
  public AlterMaterializedView withOption(String name, Object value) {
    return new DefaultAlterMaterializedView(
        keyspace, viewName, ImmutableCollections.append(options, name, value));
  }

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

  @Override
  public Map<String, Object> getOptions() {
    return options;
  }

  public CqlIdentifier getKeyspace() {
    return keyspace;
  }

  public CqlIdentifier getMaterializedView() {
    return viewName;
  }
}
