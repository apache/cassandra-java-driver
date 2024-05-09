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
import com.datastax.oss.driver.api.querybuilder.schema.AlterKeyspace;
import com.datastax.oss.driver.api.querybuilder.schema.AlterKeyspaceStart;
import com.datastax.oss.driver.internal.querybuilder.ImmutableCollections;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultAlterKeyspace implements AlterKeyspaceStart, AlterKeyspace {

  private final CqlIdentifier keyspaceName;
  private final ImmutableMap<String, Object> options;

  public DefaultAlterKeyspace(@NonNull CqlIdentifier keyspaceName) {
    this(keyspaceName, ImmutableMap.of());
  }

  public DefaultAlterKeyspace(
      @NonNull CqlIdentifier keyspaceName, @NonNull ImmutableMap<String, Object> options) {
    this.keyspaceName = keyspaceName;
    this.options = options;
  }

  @NonNull
  @Override
  public AlterKeyspace withReplicationOptions(@NonNull Map<String, Object> replicationOptions) {
    return withOption("replication", replicationOptions);
  }

  @NonNull
  @Override
  public AlterKeyspace withOption(@NonNull String name, @NonNull Object value) {
    return new DefaultAlterKeyspace(
        keyspaceName, ImmutableCollections.append(options, name, value));
  }

  @NonNull
  @Override
  public String asCql() {
    return "ALTER KEYSPACE " + keyspaceName.asCql(true) + OptionsUtils.buildOptions(options, true);
  }

  @NonNull
  @Override
  public Map<String, Object> getOptions() {
    return options;
  }

  @NonNull
  public CqlIdentifier getKeyspace() {
    return keyspaceName;
  }

  @Override
  public String toString() {
    return asCql();
  }
}
