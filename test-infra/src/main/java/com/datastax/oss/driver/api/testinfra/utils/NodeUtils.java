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
package com.datastax.oss.driver.api.testinfra.utils;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeUtils {

  private static final Logger logger = LoggerFactory.getLogger(NodeUtils.class);

  private static final int TEST_BASE_NODE_WAIT = 60;

  public static void waitForUp(Node node) {
    waitFor(node, TEST_BASE_NODE_WAIT, NodeState.UP);
  }

  public static void waitForUp(Node node, int timeoutSeconds) {
    waitFor(node, timeoutSeconds, NodeState.UP);
  }

  public static void waitForDown(Node node) {
    waitFor(node, TEST_BASE_NODE_WAIT * 3, NodeState.DOWN);
  }

  public static void waitForDown(Node node, int timeoutSeconds) {
    waitFor(node, timeoutSeconds, NodeState.DOWN);
  }

  public static void waitFor(Node node, int timeoutSeconds, NodeState nodeState) {
    logger.debug("Waiting for node {} to enter state {}", node, nodeState);
    await()
        .pollInterval(100, MILLISECONDS)
        .atMost(timeoutSeconds, SECONDS)
        .until(() -> node.getState().equals(nodeState));
  }
}
