// Copyright (c) YugaByte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the
// License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
// express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//
package com.yugabyte.oss.driver.api.core;

import com.datastax.oss.driver.api.core.metadata.Node;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The partition split for a table. It maintains a map from start key to partition metadata for each
 * partition split of the table.
 */
public class TableSplitMetadata {
  private static final Logger logger = LoggerFactory.getLogger(TableSplitMetadata.class);

  // Map from start-key to partition metadata representing the partition split of a table.
  private final NavigableMap<Integer, PartitionMetadata> partitionMap;

  /** Creates a new {@code TableSplitMetadata}. */
  public TableSplitMetadata() {
    this.partitionMap = new TreeMap<Integer, PartitionMetadata>();
  }

  /**
   * Returns the partition metadata for the partition key in the given table.
   *
   * @param key the partition key
   * @return the partition metadata for the partition key, or {@code null} when there is no
   *     partition information available
   */
  public PartitionMetadata getPartitionMetadata(int key) {
    Map.Entry<Integer, PartitionMetadata> entry = partitionMap.floorEntry(key);
    if (entry == null) {
      return null;
    }

    PartitionMetadata partition = entry.getValue();
    logger.debug("key " + key + " -> partition = " + partition.toString());
    return partition;
  }

  /**
   * Returns the hosts for the partition key in the given table.
   *
   * @param key the partition key
   * @return the hosts for the partition key, or an empty list when there is no hosts information
   *     available
   */
  public List<Node> getHosts(int key) {
    PartitionMetadata partitionMetadata = getPartitionMetadata(key);
    if (partitionMetadata == null) {
      return Collections.emptyList();
    }
    return partitionMetadata.getHosts();
  }

  /**
   * Returns the partition map defining this table split.
   *
   * @return the partition map
   */
  public NavigableMap<Integer, PartitionMetadata> getPartitionMap() {
    return partitionMap;
  }
}
