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
package com.datastax.oss.driver.internal.core.loadbalancing;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Set;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default load balancing policy implementation.
 *
 * <p>To activate this policy, modify the {@code basic.load-balancing-policy} section in the driver
 * configuration, for example:
 *
 * <pre>
 * datastax-java-driver {
 *   basic.load-balancing-policy {
 *     class = DefaultLoadBalancingPolicy
 *     local-datacenter = datacenter1
 *   }
 * }
 * </pre>
 *
 * See {@code reference.conf} (in the manual or core driver JAR) for more details.
 */
@ThreadSafe
public class DefaultLoadBalancingPolicy extends LoadBalancingPolicyBase {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultLoadBalancingPolicy.class);

  public DefaultLoadBalancingPolicy(@NonNull DriverContext context, @NonNull String profileName) {
    super((InternalDriverContext) context, profileName);
  }

  @NonNull
  @Override
  protected String inferLocalDatacenter() {
    if (context.getMetadataManager().wasImplicitContactPoint()) {
      // We only allow automatic inference of the local DC in this case
      Set<DefaultNode> contactPoints = context.getMetadataManager().getContactPoints();
      assert contactPoints.size() == 1;
      Node contactPoint = contactPoints.iterator().next();
      String localDc = contactPoint.getDatacenter();
      if (localDc != null) {
        LOG.debug("[{}] Local DC set from contact point {}: {}", logPrefix, contactPoint, localDc);
        return localDc;
      }
    }
    throw new IllegalStateException(
        "You provided explicit contact points, the local DC must be specified (see "
            + DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER.getPath()
            + " in the config)");
  }
}
