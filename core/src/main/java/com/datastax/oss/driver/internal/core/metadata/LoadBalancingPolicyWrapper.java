/*
 * Copyright (C) 2017-2017 DataStax Inc.
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

import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps the user-provided LBP for internal use. This serves multiple purposes:
 *
 * <ul>
 * <li>help enforce the guarantee that init is called exactly once, and before any other method.
 * <li>handle the early stages of initialization (before first actual connect), where the LBP is not
 *     ready yet.
 * <li>process distance events.
 * </ul>
 */
public class LoadBalancingPolicyWrapper implements LoadBalancingPolicy.DistanceReporter {
  private static final Logger LOG = LoggerFactory.getLogger(LoadBalancingPolicyWrapper.class);

  private final InternalDriverContext context;
  private final LoadBalancingPolicy policy;
  private AtomicBoolean isInit = new AtomicBoolean();

  public LoadBalancingPolicyWrapper(InternalDriverContext context, LoadBalancingPolicy policy) {
    this.context = context;
    this.policy = policy;
  }

  public void init(Set<Node> nodes) {
    if (isInit.compareAndSet(false, true)) {
      policy.init(nodes, this);
    }
  }

  public Queue<Node> newQueryPlan() {
    if (isInit.get()) {
      return policy.newQueryPlan();
    } else {
      // Still in early initialization: retrieve nodes from the metadata (at this stage it's the
      // contact points).
      List<Node> nodes = new ArrayList<>();
      nodes.addAll(context.metadataManager().getMetadata().getNodes().values());
      Collections.shuffle(nodes);
      return new ConcurrentLinkedQueue<>(nodes);
    }
  }

  @Override
  public void setDistance(Node node, NodeDistance distance) {
    LOG.debug("LBP changed distance of {} to {}", node, distance);
    DefaultNode defaultNode = (DefaultNode) node;
    defaultNode.distance = distance;
    context.eventBus().fire(new DistanceEvent(distance, defaultNode));
  }
}
