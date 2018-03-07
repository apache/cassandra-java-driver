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
import com.datastax.oss.driver.api.querybuilder.schema.AlterKeyspace;
import com.datastax.oss.driver.api.querybuilder.schema.AlterKeyspaceStart;
import com.datastax.oss.driver.internal.querybuilder.ImmutableCollections;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

public class DefaultAlterKeyspace implements AlterKeyspaceStart, AlterKeyspace {

  private final CqlIdentifier keyspaceName;
  private final ImmutableMap<String, Object> options;

  public DefaultAlterKeyspace(CqlIdentifier keyspaceName) {
    this(keyspaceName, ImmutableMap.of());
  }

  public DefaultAlterKeyspace(CqlIdentifier keyspaceName, ImmutableMap<String, Object> options) {
    this.keyspaceName = keyspaceName;
    this.options = options;
  }

  @Override
  public AlterKeyspace withReplicationOptions(Map<String, Object> replicationOptions) {
    return withOption("replication", replicationOptions);
  }

  @Override
  public AlterKeyspace withOption(String name, Object value) {
    return new DefaultAlterKeyspace(
        keyspaceName, ImmutableCollections.append(options, name, value));
  }

  @Override
  public String asCql() {
    return "ALTER KEYSPACE " + keyspaceName.asCql(true) + OptionsUtils.buildOptions(options, true);
  }

  @Override
  public Map<String, Object> getOptions() {
    return options;
  }

  public CqlIdentifier getKeyspace() {
    return keyspaceName;
  }

  @Override
  public String toString() {
    return asCql();
  }
}
