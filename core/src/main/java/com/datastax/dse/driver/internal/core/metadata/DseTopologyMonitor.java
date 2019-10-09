/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.metadata;

import com.datastax.dse.driver.api.core.metadata.DseNodeProperties;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultNodeInfo;
import com.datastax.oss.driver.internal.core.metadata.DefaultTopologyMonitor;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.InetSocketAddress;
import java.util.Set;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class DseTopologyMonitor extends DefaultTopologyMonitor {

  public DseTopologyMonitor(InternalDriverContext context) {
    super(context);
  }

  @NonNull
  @Override
  protected DefaultNodeInfo.Builder nodeInfoBuilder(
      @NonNull AdminRow row,
      @Nullable InetSocketAddress broadcastRpcAddress,
      @NonNull EndPoint localEndPoint) {

    // Fill default fields from standard columns:
    DefaultNodeInfo.Builder builder =
        super.nodeInfoBuilder(row, broadcastRpcAddress, localEndPoint);

    // Handle DSE-specific columns
    String rawVersion = row.getString("dse_version");
    if (rawVersion != null) {
      builder.withExtra(DseNodeProperties.DSE_VERSION, Version.parse(rawVersion));
    }

    ImmutableSet.Builder<String> workloadsBuilder = ImmutableSet.builder();
    Boolean legacyGraph = row.getBoolean("graph"); // DSE 5.0
    if (legacyGraph != null && legacyGraph) {
      workloadsBuilder.add("Graph");
    }
    String legacyWorkload = row.getString("workload"); // DSE 5.0 (other than graph)
    if (legacyWorkload != null) {
      workloadsBuilder.add(legacyWorkload);
    }
    Set<String> modernWorkloads = row.getSetOfString("workloads"); // DSE 5.1+
    if (modernWorkloads != null) {
      workloadsBuilder.addAll(modernWorkloads);
    }
    builder.withExtra(DseNodeProperties.DSE_WORKLOADS, workloadsBuilder.build());

    builder
        .withExtra(DseNodeProperties.SERVER_ID, row.getString("server_id"))
        .withExtra(DseNodeProperties.NATIVE_TRANSPORT_PORT, row.getInteger("native_transport_port"))
        .withExtra(
            DseNodeProperties.NATIVE_TRANSPORT_PORT_SSL,
            row.getInteger("native_transport_port_ssl"))
        .withExtra(DseNodeProperties.STORAGE_PORT, row.getInteger("storage_port"))
        .withExtra(DseNodeProperties.STORAGE_PORT_SSL, row.getInteger("storage_port_ssl"))
        .withExtra(DseNodeProperties.JMX_PORT, row.getInteger("jmx_port"));

    return builder;
  }
}
