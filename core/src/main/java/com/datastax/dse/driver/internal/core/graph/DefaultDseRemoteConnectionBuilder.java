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
