/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.insights;

import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE;

import com.datastax.dse.driver.internal.core.context.DseDriverContext;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

class DataCentersFinder {

  Set<String> getDataCenters(DseDriverContext driverContext) {
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
