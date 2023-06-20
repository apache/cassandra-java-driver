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
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistanceEvaluator;
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
 * A {@link NodeDistanceEvaluatorHelper} implementation that fetches the user-supplied evaluator, if
 * any, from the programmatic configuration API, or else, from the driver configuration. If no
 * user-supplied evaluator can be retrieved, a dummy evaluator will be used which always evaluates
 * null distances.
 */
@ThreadSafe
public class DefaultNodeDistanceEvaluatorHelper implements NodeDistanceEvaluatorHelper {

  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultNodeDistanceEvaluatorHelper.class);

  @NonNull protected final InternalDriverContext context;
  @NonNull protected final DriverExecutionProfile profile;
  @NonNull protected final String logPrefix;

  public DefaultNodeDistanceEvaluatorHelper(
      @NonNull InternalDriverContext context,
      @NonNull DriverExecutionProfile profile,
      @NonNull String logPrefix) {
    this.context = context;
    this.profile = profile;
    this.logPrefix = logPrefix;
  }

  @NonNull
  @Override
  public NodeDistanceEvaluator createNodeDistanceEvaluator(
      @Nullable String localDc, @NonNull Map<UUID, Node> nodes) {
    NodeDistanceEvaluator nodeDistanceEvaluatorFromConfig = nodeDistanceEvaluatorFromConfig();
    return (node, dc) -> {
      NodeDistance distance = nodeDistanceEvaluatorFromConfig.evaluateDistance(node, dc);
      if (distance != null) {
        LOG.debug("[{}] Evaluator assigned distance {} to node {}", logPrefix, distance, node);
      } else {
        LOG.debug("[{}] Evaluator did not assign a distance to node {}", logPrefix, node);
      }
      return distance;
    };
  }

  @NonNull
  protected NodeDistanceEvaluator nodeDistanceEvaluatorFromConfig() {
    NodeDistanceEvaluator evaluator = context.getNodeDistanceEvaluator(profile.getName());
    if (evaluator != null) {
      LOG.debug("[{}] Node distance evaluator set programmatically", logPrefix);
    } else {
      evaluator =
          Reflection.buildFromConfig(
                  context,
                  profile.getName(),
                  DefaultDriverOption.LOAD_BALANCING_DISTANCE_EVALUATOR_CLASS,
                  NodeDistanceEvaluator.class)
              .orElse(null);
      if (evaluator != null) {
        LOG.debug("[{}] Node distance evaluator set from configuration", logPrefix);
      } else {
        @SuppressWarnings({"unchecked", "deprecation"})
        Predicate<Node> nodeFilterFromConfig =
            Reflection.buildFromConfig(
                    context,
                    profile.getName(),
                    DefaultDriverOption.LOAD_BALANCING_FILTER_CLASS,
                    Predicate.class)
                .orElse(null);
        if (nodeFilterFromConfig != null) {
          evaluator = new NodeFilterToDistanceEvaluatorAdapter(nodeFilterFromConfig);
          LOG.debug(
              "[{}] Node distance evaluator set from deprecated node filter configuration",
              logPrefix);
        }
      }
    }
    if (evaluator == null) {
      evaluator = PASS_THROUGH_DISTANCE_EVALUATOR;
    }
    return evaluator;
  }
}
