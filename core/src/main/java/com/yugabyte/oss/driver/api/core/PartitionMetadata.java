// Copyright (c) YugaByte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the
//  License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//
package com.yugabyte.oss.driver.api.core;

import com.datastax.oss.driver.api.core.metadata.Node;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * The metadata for one table partition. It maintains the partition's range (start and end key) and
 * hosts (leader and followers).
 */
public class PartitionMetadata {

  // The partition start -- inclusive bound.
  private final Integer startKey;
  // The partition end -- exclusive bound.
  private final Integer endKey;
  // The list of hosts -- first one should be the leader, then the followers in no
  // particular
  // order.
  // TODO We should make the leader explicit here and in the return type of the
  // getQueryPlan
  // method
  // to be able to distinguish the case where the leader is missing so the hosts
  // are all
  // followers.
  private final List<Node> hosts;

  /** Creates a new {@code PartitionMetadata}. */
  public PartitionMetadata(Integer startKey, Integer endKey, List<Node> hosts) {
    this.startKey = startKey;
    this.endKey = endKey;
    this.hosts = (hosts != null) ? hosts : Collections.<Node>emptyList();
  }

  /**
   * Returns the start key (inclusive lower bound) of this partition.
   *
   * @return the start key
   */
  public Integer getStartKey() {
    return startKey;
  }

  /**
   * Returns the end key (exclusive upper bound) of this partition.
   *
   * @return the end key
   */
  public Integer getEndKey() {
    return endKey;
  }

  /**
   * Returns the hosts (leader and followers) of this partition.
   *
   * @return the hosts, or the empty list if host list is unavailable.
   */
  public List<Node> getHosts() {
    return hosts;
  }

  @Override
  public String toString() {
    return String.format("[%d, %d) -> %s", startKey, endKey, Arrays.toString(hosts.toArray()));
  }
}
