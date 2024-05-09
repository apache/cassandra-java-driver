/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link LocalDcHelper} that fetches the local datacenter from the
 * programmatic configuration API, or else, from the driver configuration. If no user-supplied
 * datacenter can be retrieved, it returns {@link Optional#empty empty}.
 */
@ThreadSafe
public class OptionalLocalDcHelper implements LocalDcHelper {

  private static final Logger LOG = LoggerFactory.getLogger(OptionalLocalDcHelper.class);

  @NonNull protected final InternalDriverContext context;
  @NonNull protected final DriverExecutionProfile profile;
  @NonNull protected final String logPrefix;

  public OptionalLocalDcHelper(
      @NonNull InternalDriverContext context,
      @NonNull DriverExecutionProfile profile,
      @NonNull String logPrefix) {
    this.context = context;
    this.profile = profile;
    this.logPrefix = logPrefix;
  }

  /**
   * @return The local datacenter from the programmatic configuration API, or from the driver
   *     configuration; {@link Optional#empty empty} if none found.
   */
  @Override
  @NonNull
  public Optional<String> discoverLocalDc(@NonNull Map<UUID, Node> nodes) {
    String localDc = context.getLocalDatacenter(profile.getName());
    if (localDc != null) {
      LOG.debug("[{}] Local DC set programmatically: {}", logPrefix, localDc);
      checkLocalDatacenterCompatibility(localDc, context.getMetadataManager().getContactPoints());
      return Optional.of(localDc);
    } else if (profile.isDefined(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER)) {
      localDc = profile.getString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER);
      LOG.debug("[{}] Local DC set from configuration: {}", logPrefix, localDc);
      checkLocalDatacenterCompatibility(localDc, context.getMetadataManager().getContactPoints());
      return Optional.of(localDc);
    } else {
      LOG.debug("[{}] Local DC not set, DC awareness will be disabled", logPrefix);
      return Optional.empty();
    }
  }

  /**
   * Checks if the contact points are compatible with the local datacenter specified either through
   * configuration, or programmatically.
   *
   * <p>The default implementation logs a warning when a contact point reports a datacenter
   * different from the local one, and only for the default profile.
   *
   * @param localDc The local datacenter, as specified in the config, or programmatically.
   * @param contactPoints The contact points provided when creating the session.
   */
  protected void checkLocalDatacenterCompatibility(
      @NonNull String localDc, Set<? extends Node> contactPoints) {
    if (profile.getName().equals(DriverExecutionProfile.DEFAULT_NAME)) {
      Set<Node> badContactPoints = new LinkedHashSet<>();
      for (Node node : contactPoints) {
        if (!Objects.equals(localDc, node.getDatacenter())) {
          badContactPoints.add(node);
        }
      }
      if (!badContactPoints.isEmpty()) {
        LOG.warn(
            "[{}] You specified {} as the local DC, but some contact points are from a different DC: {}; "
                + "please provide the correct local DC, or check your contact points",
            logPrefix,
            localDc,
            formatNodesAndDcs(badContactPoints));
      }
    }
  }

  /**
   * Formats the given nodes as a string detailing each contact point and its datacenter, for
   * informational purposes.
   */
  @NonNull
  protected String formatNodesAndDcs(Iterable<? extends Node> nodes) {
    List<String> l = new ArrayList<>();
    for (Node node : nodes) {
      l.add(node + "=" + node.getDatacenter());
    }
    return String.join(", ", l);
  }

  /**
   * Formats the given nodes as a string detailing each distinct datacenter, for informational
   * purposes.
   */
  @NonNull
  protected String formatDcs(Iterable<? extends Node> nodes) {
    List<String> l = new ArrayList<>();
    for (Node node : nodes) {
      if (node.getDatacenter() != null) {
        l.add(node.getDatacenter());
      }
    }
    return String.join(", ", new TreeSet<>(l));
  }
}
