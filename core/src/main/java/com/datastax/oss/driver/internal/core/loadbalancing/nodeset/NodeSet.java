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
package com.datastax.oss.driver.internal.core.loadbalancing.nodeset;

import com.datastax.oss.driver.api.core.metadata.Node;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Set;
import net.jcip.annotations.ThreadSafe;

/**
 * A thread-safe abstraction around a map of nodes per datacenter, to facilitate node management by
 * load balancing policies.
 */
@ThreadSafe
public interface NodeSet {

  /**
   * Adds the given node to this set.
   *
   * <p>If this set was initialized with datacenter awareness, the node will be added to its
   * datacenter's specific set; otherwise, the node is added to a general set containing all nodes
   * in the cluster.
   *
   * @param node The node to add.
   * @return true if the node was added, false otherwise (because it was already present).
   */
  boolean add(@NonNull Node node);

  /**
   * Removes the node from the set.
   *
   * @param node The node to remove.
   * @return true if the node was removed, false otherwise (because it was not present).
   */
  boolean remove(@NonNull Node node);

  /**
   * Returns the current nodes in the given datacenter.
   *
   * <p>If this set was initialized with datacenter awareness, the returned set will contain only
   * nodes pertaining to the given datacenter; otherwise, the given datacenter name is ignored and
   * the returned set will contain all nodes in the cluster.
   *
   * @param dc The datacenter name, or null if the datacenter name is not known, or irrelevant.
   * @return the current nodes in the given datacenter.
   */
  @NonNull
  Set<Node> dc(@Nullable String dc);

  /**
   * Returns the current datacenter names known to this set. If datacenter awareness has been
   * disabled, this method returns an empty set.
   */
  Set<String> dcs();
}
