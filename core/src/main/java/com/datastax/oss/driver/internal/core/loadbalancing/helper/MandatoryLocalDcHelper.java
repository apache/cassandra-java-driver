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

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link LocalDcHelper} that fetches the user-supplied datacenter, if any,
 * from the programmatic configuration API, or else, from the driver configuration. If no local
 * datacenter is explicitly defined, this implementation will consider two distinct situations:
 *
 * <ol>
 *   <li>If no explicit contact points were provided, this implementation will infer the local
 *       datacenter from the implicit contact point (localhost).
 *   <li>If explicit contact points were provided however, this implementation will throw {@link
 *       IllegalStateException}.
 * </ol>
 */
@ThreadSafe
public class MandatoryLocalDcHelper extends OptionalLocalDcHelper {

  private static final Logger LOG = LoggerFactory.getLogger(MandatoryLocalDcHelper.class);

  public MandatoryLocalDcHelper(
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
    Set<DefaultNode> contactPoints = context.getMetadataManager().getContactPoints();
    if (context.getMetadataManager().wasImplicitContactPoint()) {
      // We only allow automatic inference of the local DC in this specific case
      assert contactPoints.size() == 1;
      Node contactPoint = contactPoints.iterator().next();
      String localDc = contactPoint.getDatacenter();
      if (localDc != null) {
        LOG.debug(
            "[{}] Local DC set from implicit contact point {}: {}",
            logPrefix,
            contactPoint,
            localDc);
        return Optional.of(localDc);
      } else {
        throw new IllegalStateException(
            "The local DC could not be inferred from implicit contact point, please set it explicitly (see "
                + DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER.getPath()
                + " in the config, or set it programmatically with SessionBuilder.withLocalDatacenter)");
      }
    } else {
      throw new IllegalStateException(
          "Since you provided explicit contact points, the local DC must be explicitly set (see "
              + DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER.getPath()
              + " in the config, or set it programmatically with SessionBuilder.withLocalDatacenter). "
              + "Current contact points are: "
              + formatNodesAndDcs(contactPoints)
              + ". Current DCs in this cluster are: "
              + formatDcs(nodes.values()));
    }
  }
}
