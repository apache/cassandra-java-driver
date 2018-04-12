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
package com.datastax.oss.driver.internal.core.pool;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import java.util.concurrent.CompletionStage;
import net.jcip.annotations.ThreadSafe;

/** Just a level of indirection to make testing easier. */
@ThreadSafe
public class ChannelPoolFactory {
  public CompletionStage<ChannelPool> init(
      Node node,
      CqlIdentifier keyspaceName,
      NodeDistance distance,
      InternalDriverContext context,
      String sessionLogPrefix) {
    return ChannelPool.init(node, keyspaceName, distance, context, sessionLogPrefix);
  }
}
