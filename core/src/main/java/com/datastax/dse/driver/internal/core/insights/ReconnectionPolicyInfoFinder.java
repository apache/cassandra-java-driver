/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.insights;

import com.datastax.dse.driver.internal.core.insights.schema.ReconnectionPolicyInfo;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.internal.core.connection.ConstantReconnectionPolicy;
import com.datastax.oss.driver.internal.core.connection.ExponentialReconnectionPolicy;
import java.util.HashMap;
import java.util.Map;

class ReconnectionPolicyInfoFinder {
  ReconnectionPolicyInfo getReconnectionPolicyInfo(
      ReconnectionPolicy reconnectionPolicy, DriverExecutionProfile executionProfile) {
    Class<? extends ReconnectionPolicy> reconnectionPolicyClass = reconnectionPolicy.getClass();
    String type = reconnectionPolicyClass.getSimpleName();
    String namespace = PackageUtil.getNamespace(reconnectionPolicyClass);
    Map<String, Object> options = new HashMap<>();
    if (reconnectionPolicy instanceof ConstantReconnectionPolicy) {
      options.put(
          "delayMs",
          executionProfile.getDuration(DefaultDriverOption.RECONNECTION_BASE_DELAY).toMillis());
    } else if (reconnectionPolicy instanceof ExponentialReconnectionPolicy) {
      ExponentialReconnectionPolicy exponentialReconnectionPolicy =
          (ExponentialReconnectionPolicy) reconnectionPolicy;
      options.put("maxDelayMs", exponentialReconnectionPolicy.getMaxDelayMs());
      options.put("baseDelayMs", exponentialReconnectionPolicy.getBaseDelayMs());
      options.put("maxAttempts", exponentialReconnectionPolicy.getMaxAttempts());
    }
    return new ReconnectionPolicyInfo(type, options, namespace);
  }
}
