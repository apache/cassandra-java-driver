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
package com.datastax.dse.driver.api.querybuilder.schema;

import com.datastax.oss.driver.api.querybuilder.schema.KeyspaceOptions;
import com.datastax.oss.driver.api.querybuilder.schema.KeyspaceReplicationOptions;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;

public interface AlterDseKeyspaceStart
    extends KeyspaceOptions<AlterDseKeyspace>, KeyspaceReplicationOptions<AlterDseKeyspace> {

  @NonNull
  @Override
  AlterDseKeyspace withOption(@NonNull String name, @NonNull Object value);

  /**
   * Adjusts durable writes configuration for this keyspace. If set to false, data written to the
   * keyspace will bypass the commit log.
   */
  @NonNull
  @Override
  AlterDseKeyspace withDurableWrites(boolean durableWrites);

  /** Adjusts the graph engine that will be used to interpret this keyspace. */
  @NonNull
  AlterDseKeyspace withGraphEngine(String graphEngine);

  /**
   * Adds 'replication' options. One should only use this when they have a custom replication
   * strategy, otherwise it is advisable to use {@link #withSimpleStrategy(int)} or {@link
   * #withNetworkTopologyStrategy(Map)}.
   */
  @NonNull
  @Override
  AlterDseKeyspace withReplicationOptions(@NonNull Map<String, Object> replicationOptions);

  /**
   * Adds SimpleStrategy replication options with the given replication factor.
   *
   * <p>Note that using this will overwrite any previous use of this method or {@link
   * #withNetworkTopologyStrategy(Map)}.
   */
  @NonNull
  @Override
  AlterDseKeyspace withSimpleStrategy(int replicationFactor);

  /**
   * Adds NetworkTopologyStrategy replication options with the given data center replication
   * factors.
   *
   * <p>Note that using this will overwrite any previous use of this method or {@link
   * #withSimpleStrategy(int)}.
   *
   * @param replications Mapping of data center name to replication factor to use for that data
   *     center.
   */
  @NonNull
  @Override
  AlterDseKeyspace withNetworkTopologyStrategy(@NonNull Map<String, Integer> replications);
}
