/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.metadata.token;

import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.token.DefaultReplicationStrategyFactory;
import com.datastax.oss.driver.internal.core.metadata.token.ReplicationStrategy;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import java.util.Map;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class DseReplicationStrategyFactory extends DefaultReplicationStrategyFactory {
  public DseReplicationStrategyFactory(InternalDriverContext context) {
    super(context);
  }

  @Override
  public ReplicationStrategy newInstance(Map<String, String> replicationConfig) {
    String strategyClass = replicationConfig.get("class");
    Preconditions.checkNotNull(
        strategyClass, "Missing replication strategy class in " + replicationConfig);
    switch (strategyClass) {
      case "org.apache.cassandra.locator.EverywhereStrategy":
        return new EverywhereStrategy();
      default:
        return super.newInstance(replicationConfig);
    }
  }
}
