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
import com.datastax.oss.driver.internal.core.util.Reflection;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link NodeFilterHelper} implementation that fetches the user-supplied filter, if any, from the
 * programmatic configuration API, or else, from the driver configuration. If no user-supplied
 * filter can be retrieved, a dummy filter will be used which accepts all nodes unconditionally.
 *
 * <p>Note that, regardless of the filter supplied by the end user, if a local datacenter is defined
 * the filter returned by this implementation will always reject nodes that report a datacenter
 * different from the local one.
 */
@ThreadSafe
public class DefaultNodeFilterHelper implements NodeFilterHelper {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultNodeFilterHelper.class);

  @NonNull protected final InternalDriverContext context;
  @NonNull protected final DriverExecutionProfile profile;
  @NonNull protected final String logPrefix;

  public DefaultNodeFilterHelper(
      @NonNull InternalDriverContext context,
      @NonNull DriverExecutionProfile profile,
      @NonNull String logPrefix) {
    this.context = context;
    this.profile = profile;
    this.logPrefix = logPrefix;
  }

  @NonNull
  @Override
  public Predicate<Node> createNodeFilter(
      @Nullable String localDc, @NonNull Map<UUID, Node> nodes) {
    Predicate<Node> filterFromConfig = nodeFilterFromConfig();
    return node -> {
      if (!filterFromConfig.test(node)) {
        LOG.debug(
            "[{}] Ignoring {} because it doesn't match the user-provided predicate",
            logPrefix,
            node);
        return false;
      } else {
        return true;
      }
    };
  }

  @NonNull
  protected Predicate<Node> nodeFilterFromConfig() {
    Predicate<Node> filter = context.getNodeFilter(profile.getName());
    if (filter != null) {
      LOG.debug("[{}] Node filter set programmatically", logPrefix);
    } else {
      @SuppressWarnings("unchecked")
      Predicate<Node> filterFromConfig =
          Reflection.buildFromConfig(
                  context,
                  profile.getName(),
                  DefaultDriverOption.LOAD_BALANCING_FILTER_CLASS,
                  Predicate.class)
              .orElse(INCLUDE_ALL_NODES);
      filter = filterFromConfig;
      LOG.debug("[{}] Node filter set from configuration", logPrefix);
    }
    return filter;
  }
}
