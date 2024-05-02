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
package com.datastax.dse.driver.api.core.metadata.schema;

import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.ScriptBuilder;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.Optional;

/**
 * Specialized keyspace metadata, that handles the graph-specific properties introduced in DSE 6.8.
 *
 * <p>This type only exists to avoid breaking binary compatibility. When the driver is connected to
 * a DSE cluster, all the {@link KeyspaceMetadata} instances it returns can be safely downcast to
 * this interface.
 */
public interface DseGraphKeyspaceMetadata extends DseKeyspaceMetadata {

  /** The graph engine that will be used to interpret this keyspace. */
  @NonNull
  Optional<String> getGraphEngine();

  @NonNull
  @Override
  default String describe(boolean pretty) {
    ScriptBuilder builder = new ScriptBuilder(pretty);
    if (isVirtual()) {
      builder.append("/* VIRTUAL ");
    } else {
      builder.append("CREATE ");
    }
    builder
        .append("KEYSPACE ")
        .append(getName())
        .append(" WITH replication = { 'class' : '")
        .append(getReplication().get("class"))
        .append("'");
    for (Map.Entry<String, String> entry : getReplication().entrySet()) {
      if (!entry.getKey().equals("class")) {
        builder
            .append(", '")
            .append(entry.getKey())
            .append("': '")
            .append(entry.getValue())
            .append("'");
      }
    }
    builder.append(" } AND durable_writes = ").append(Boolean.toString(isDurableWrites()));
    getGraphEngine()
        .ifPresent(
            graphEngine -> builder.append(" AND graph_engine ='").append(graphEngine).append("'"));
    builder.append(";");
    if (isVirtual()) {
      builder.append(" */");
    }
    return builder.build();
  }
}
