/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.metadata.token;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.internal.core.metadata.token.ReplicationStrategy;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSetMultimap;
import com.datastax.oss.driver.shaded.guava.common.collect.SetMultimap;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class EverywhereStrategy implements ReplicationStrategy {

  @Override
  public SetMultimap<Token, Node> computeReplicasByToken(
      Map<Token, Node> tokenToPrimary, List<Token> ring) {
    ImmutableSetMultimap.Builder<Token, Node> result = ImmutableSetMultimap.builder();
    Collection<Node> nodes = tokenToPrimary.values();
    for (Token token : tokenToPrimary.keySet()) {
      result = result.putAll(token, nodes);
    }
    return result.build();
  }
}
