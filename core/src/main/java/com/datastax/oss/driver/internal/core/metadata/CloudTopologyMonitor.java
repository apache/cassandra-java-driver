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
package com.datastax.oss.driver.internal.core.metadata;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.UUID;

public class CloudTopologyMonitor extends DefaultTopologyMonitor {

  private final InetSocketAddress cloudProxyAddress;

  public CloudTopologyMonitor(InternalDriverContext context, InetSocketAddress cloudProxyAddress) {
    super(context);
    this.cloudProxyAddress = cloudProxyAddress;
  }

  @NonNull
  @Override
  protected EndPoint buildNodeEndPoint(
      @NonNull AdminRow row,
      @Nullable InetSocketAddress broadcastRpcAddress,
      @NonNull EndPoint localEndPoint) {
    UUID hostId = Objects.requireNonNull(row.getUuid("host_id"));
    return new SniEndPoint(cloudProxyAddress, hostId.toString());
  }
}
