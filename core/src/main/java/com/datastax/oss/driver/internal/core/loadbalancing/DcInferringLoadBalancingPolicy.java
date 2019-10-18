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
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link LoadBalancingPolicy} that infers the local datacenter from the
 * contact points, if no datacenter was provided neither through configuration nor programmatically.
 *
 * <p>This class is not recommended for normal users, who should always prefer {@link
 * DefaultLoadBalancingPolicy}.
 */
@ThreadSafe
public class DcInferringLoadBalancingPolicy extends BasicLoadBalancingPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(DcInferringLoadBalancingPolicy.class);

  public DcInferringLoadBalancingPolicy(
      @NonNull DriverContext context, @NonNull String profileName) {
    super(context, profileName);
  }

  /**
   * {@inheritDoc}
   *
   * <p>This implementation infers the local datacenter from the contact points: if all contact
   * points share the same datacenter, that datacenter is returned. If the contact points are from
   * different datacenters, or if no contact points reported any datacenter, an {@link
   * IllegalStateException} is thrown.
   *
   * @return The local datacenter; always present.
   * @throws IllegalStateException if the local datacenter could not be inferred.
   */
  @NonNull
  @Override
  protected Optional<String> inferLocalDatacenter() {
    Optional<String> optionalLocalDc = super.inferLocalDatacenter();
    if (optionalLocalDc.isPresent()) {
      return optionalLocalDc;
    }
    Set<String> datacenters = new HashSet<>();
    Set<? extends Node> contactPoints = context.getMetadataManager().getContactPoints();
    for (Node node : contactPoints) {
      String datacenter = node.getDatacenter();
      if (datacenter != null) {
        datacenters.add(datacenter);
      }
    }
    if (datacenters.size() == 1) {
      String localDc = datacenters.iterator().next();
      LOG.info("[{}] Inferred local DC from contact points: {}", logPrefix, localDc);
      return Optional.of(localDc);
    }
    if (datacenters.isEmpty()) {
      throw new IllegalStateException(
          "The local DC could not be inferred from contact points, please set it explicitly (see "
              + DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER.getPath()
              + " in the config, or set it programmatically with SessionBuilder.withLocalDatacenter)");
    }
    throw new IllegalStateException(
        String.format(
            "No local DC was provided, but the contact points are from different DCs: %s; "
                + "please set the local DC explicitly (see "
                + DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER.getPath()
                + " in the config, or set it programmatically with SessionBuilder.withLocalDatacenter)",
            formatNodes(contactPoints)));
  }
}
