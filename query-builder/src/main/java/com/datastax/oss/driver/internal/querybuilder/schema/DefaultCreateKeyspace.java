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
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspaceStart;
import com.datastax.oss.driver.internal.querybuilder.ImmutableCollections;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.Map;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultCreateKeyspace implements CreateKeyspace, CreateKeyspaceStart {

  private final CqlIdentifier keyspaceName;
  private final boolean ifNotExists;
  private final ImmutableMap<String, Object> options;

  public DefaultCreateKeyspace(CqlIdentifier keyspaceName) {
    this(keyspaceName, false, ImmutableMap.of());
  }

  public DefaultCreateKeyspace(
      CqlIdentifier keyspaceName, boolean ifNotExists, ImmutableMap<String, Object> options) {
    this.keyspaceName = keyspaceName;
    this.ifNotExists = ifNotExists;
    this.options = options;
  }

  @Override
  public CreateKeyspace withOption(String name, Object value) {
    return new DefaultCreateKeyspace(
        keyspaceName, ifNotExists, ImmutableCollections.append(options, name, value));
  }

  @Override
  public CreateKeyspaceStart ifNotExists() {
    return new DefaultCreateKeyspace(keyspaceName, true, options);
  }

  @Override
  public CreateKeyspace withReplicationOptions(Map<String, Object> replicationOptions) {
    return withOption("replication", replicationOptions);
  }

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

  @Override
  public Map<String, Object> getOptions() {
    return options;
  }

  public CqlIdentifier getKeyspace() {
    return keyspaceName;
  }

  public boolean isIfNotExists() {
    return ifNotExists;
  }
}
