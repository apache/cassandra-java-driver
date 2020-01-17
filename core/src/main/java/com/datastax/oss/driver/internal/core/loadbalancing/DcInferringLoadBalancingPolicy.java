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
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.loadbalancing.helper.InferringLocalDcHelper;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import net.jcip.annotations.ThreadSafe;

/**
 * An implementation of {@link LoadBalancingPolicy} that infers the local datacenter from the
 * contact points, if no datacenter was provided neither through configuration nor programmatically.
 *
 * <p>To activate this policy, modify the {@code basic.load-balancing-policy} section in the driver
 * configuration, for example:
 *
 * <pre>
 * datastax-java-driver {
 *   basic.load-balancing-policy {
 *     class = DcInferringLoadBalancingPolicy
 *     local-datacenter = datacenter1 # optional
 *   }
 * }
 * </pre>
 *
 * See {@code reference.conf} (in the manual or core driver JAR) for more details.
 *
 * <p><b>Local datacenter</b>: This implementation requires a local datacenter to be defined,
 * otherwise it will throw an {@link IllegalStateException}. A local datacenter can be supplied
 * either:
 *
 * <ol>
 *   <li>Programmatically with {@link
 *       com.datastax.oss.driver.api.core.session.SessionBuilder#withLocalDatacenter(String)
 *       SessionBuilder#withLocalDatacenter(String)};
 *   <li>Through configuration, by defining the option {@link
 *       DefaultDriverOption#LOAD_BALANCING_LOCAL_DATACENTER
 *       basic.load-balancing-policy.local-datacenter};
 *   <li>Or implicitly: in this case this implementation will infer the local datacenter from the
 *       provided contact points, if and only if they are all located in the same datacenter.
 * </ol>
 *
 * <p><b>Query plan</b>: see {@link DefaultLoadBalancingPolicy} for details on the computation of
 * query plans.
 *
 * <p><b>This class is not recommended for normal users who should always prefer {@link
 * DefaultLoadBalancingPolicy}</b>.
 */
@ThreadSafe
public class DcInferringLoadBalancingPolicy extends DefaultLoadBalancingPolicy {

  public DcInferringLoadBalancingPolicy(
      @NonNull DriverContext context, @NonNull String profileName) {
    super(context, profileName);
  }

  @NonNull
  @Override
  protected Optional<String> discoverLocalDc(@NonNull Map<UUID, Node> nodes) {
    return new InferringLocalDcHelper(context, profile, logPrefix).discoverLocalDc(nodes);
  }
}
