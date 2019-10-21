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
package com.datastax.oss.driver.internal.core.loadbalancing.helper;

import static com.datastax.oss.driver.internal.core.time.Clock.LOG;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import net.jcip.annotations.ThreadSafe;

/**
 * An implementation of {@link LocalDcHelper} that fetches the user-supplied datacenter, if any,
 * from the programmatic configuration API, or else, from the driver configuration. If no local
 * datacenter is explicitly defined, this implementation infers the local datacenter from the
 * contact points: if all contact points share the same datacenter, that datacenter is returned. If
 * the contact points are from different datacenters, or if no contact points reported any
 * datacenter, an {@link IllegalStateException} is thrown.
 */
@ThreadSafe
public class InferringLocalDcHelper extends OptionalLocalDcHelper {

  public InferringLocalDcHelper(
      @NonNull InternalDriverContext context,
      @NonNull DriverExecutionProfile profile,
      @NonNull String logPrefix) {
    super(context, profile, logPrefix);
  }

  /** @return The local datacenter; always present. */
  @NonNull
  @Override
  public Optional<String> discoverLocalDc(@NonNull Map<UUID, Node> nodes) {
    Optional<String> optionalLocalDc = super.discoverLocalDc(nodes);
    if (optionalLocalDc.isPresent()) {
      return optionalLocalDc;
    }
    Set<String> datacenters = new HashSet<>();
    Set<DefaultNode> contactPoints = context.getMetadataManager().getContactPoints();
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
            formatNodesAndDcs(contactPoints)));
  }
}
