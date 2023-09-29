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
package com.datastax.dse.driver.api.querybuilder.schema;

import com.datastax.oss.driver.api.querybuilder.BuildableQuery;
import com.datastax.oss.driver.api.querybuilder.schema.KeyspaceOptions;
import edu.umd.cs.findbugs.annotations.NonNull;

public interface CreateDseKeyspace extends BuildableQuery, KeyspaceOptions<CreateDseKeyspace> {

  @NonNull
  @Override
  CreateDseKeyspace withOption(@NonNull String name, @NonNull Object value);

  /**
   * Adjusts durable writes configuration for this keyspace. If set to false, data written to the
   * keyspace will bypass the commit log.
   */
  @NonNull
  @Override
  default CreateDseKeyspace withDurableWrites(boolean durableWrites) {
    return withOption("durable_writes", durableWrites);
  }

  /** Adjusts the graph engine that will be used to interpret this keyspace. */
  @NonNull
  default CreateDseKeyspace withGraphEngine(String graphEngine) {
    return this.withOption("graph_engine", graphEngine);
  }
}
