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

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.net.InetSocketAddress;
import java.util.Set;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Creates minimal node info about the contact points, before the first connection. */
@ThreadSafe
class InitContactPointsRefresh implements MetadataRefresh {
  private static final Logger LOG = LoggerFactory.getLogger(InitContactPointsRefresh.class);

  @VisibleForTesting final Set<InetSocketAddress> contactPoints;

  InitContactPointsRefresh(Set<InetSocketAddress> contactPoints) {
    this.contactPoints = contactPoints;
  }

  @Override
  public Result compute(
      DefaultMetadata oldMetadata, boolean tokenMapEnabled, InternalDriverContext context) {

    String logPrefix = context.sessionName();
    LOG.debug("[{}] Initializing node metadata with contact points {}", logPrefix, contactPoints);

    ImmutableMap.Builder<InetSocketAddress, Node> newNodes = ImmutableMap.builder();
    for (InetSocketAddress address : contactPoints) {
      newNodes.put(address, new DefaultNode(address, context));
    }
    return new Result(
        oldMetadata.withNodes(
            newNodes.build(),
            // At this stage there is no token map and we don't have the info to refresh it yet
            false,
            false,
            null,
            context));
  }
}
