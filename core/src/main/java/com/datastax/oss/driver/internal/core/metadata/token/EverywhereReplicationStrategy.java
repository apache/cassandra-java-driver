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
package com.datastax.oss.driver.internal.core.metadata.token;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class EverywhereReplicationStrategy implements ReplicationStrategy {

  @Override
  public Map<Token, Set<Node>> computeReplicasByToken(
      Map<Token, Node> tokenToPrimary, List<Token> ring) {
    ImmutableMap.Builder<Token, Set<Node>> result = ImmutableMap.builder();
    Set<Node> allNodes = ImmutableSet.copyOf(tokenToPrimary.values());
    for (Token token : tokenToPrimary.keySet()) {
      result = result.put(token, allNodes);
    }
    return result.build();
  }
}
