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
package com.datastax.dse.driver.internal.core.insights;

import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE;

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

class DataCentersFinder {

  Set<String> getDataCenters(InternalDriverContext driverContext) {
    return getDataCenters(
        driverContext.getMetadataManager().getMetadata().getNodes().values(),
        driverContext.getConfig().getDefaultProfile());
  }

  @VisibleForTesting
  Set<String> getDataCenters(Collection<Node> nodes, DriverExecutionProfile executionProfile) {

    int remoteConnectionsLength = executionProfile.getInt(CONNECTION_POOL_REMOTE_SIZE);

    Set<String> dataCenters = new HashSet<>();
    for (Node n : nodes) {
      NodeDistance distance = n.getDistance();

      if (distance.equals(NodeDistance.LOCAL)
          || (distance.equals(NodeDistance.REMOTE) && remoteConnectionsLength > 0)) {
        dataCenters.add(n.getDatacenter());
      }
    }
    return dataCenters;
  }
}
