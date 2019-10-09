/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.graph;

import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.dse.driver.api.core.graph.DseGraphRemoteConnectionBuilder;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import net.jcip.annotations.NotThreadSafe;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;

@NotThreadSafe
public class DefaultDseRemoteConnectionBuilder implements DseGraphRemoteConnectionBuilder {

  private final DseSession dseSession;
  private DriverExecutionProfile executionProfile;
  private String executionProfileName;

  public DefaultDseRemoteConnectionBuilder(DseSession dseSession) {
    this.dseSession = dseSession;
  }

  @Override
  public RemoteConnection build() {
    return new DseGraphRemoteConnection(dseSession, executionProfile, executionProfileName);
  }

  @Override
  public DseGraphRemoteConnectionBuilder withExecutionProfile(
      DriverExecutionProfile executionProfile) {
    this.executionProfile = executionProfile;
    return this;
  }

  @Override
  public DseGraphRemoteConnectionBuilder withExecutionProfileName(String executionProfileName) {
    this.executionProfileName = executionProfileName;
    return this;
  }
}
