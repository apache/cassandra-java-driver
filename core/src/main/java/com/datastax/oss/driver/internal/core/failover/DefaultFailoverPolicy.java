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
package com.datastax.oss.driver.internal.core.failover;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.failover.FailoverPolicy;
import com.datastax.oss.driver.internal.core.cql.Conversions;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@inheritDoc}
 *
 * <p>A failover is activated when any of the following Exceptions occcur: AllNodesFailedException,
 * DriverTimeoutException, InvalidKeyspaceException, or NoNodeAvailableException
 */
@ThreadSafe
public class DefaultFailoverPolicy implements FailoverPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultFailoverPolicy.class);

  private final String logPrefix;

  private final DriverContext context;

  public DefaultFailoverPolicy(
      @SuppressWarnings("unused") DriverContext context,
      @SuppressWarnings("unused") String profileName) {
    this.context = context;
    this.logPrefix = (context != null ? context.getSessionName() : null) + "|" + profileName;
  }

  @Override
  public void close() {
    // nothing to do
  }

  /**
   * {@inheritDoc}
   *
   * <p>Evalutes the exception and triggers a failover when any of the following Exceptions occcur:
   * AllNodesFailedException, DriverTimeoutException, InvalidKeyspaceException, or
   * NoNodeAvailableException
   */
  @Override
  public Boolean shouldFailover(Throwable throwable, Statement<?> request) {
    DriverExecutionProfile driverExecutionProfile =
        Conversions.resolveExecutionProfile(request, context);

    if (!driverExecutionProfile.isDefined(DefaultDriverOption.FAILOVER_POLICY_PROFILE)) {
      return false;
    }

    if (driverExecutionProfile
        .getString(DefaultDriverOption.FAILOVER_POLICY_PROFILE)
        .equals(request.getExecutionProfileName())) {
      return false;
    }

    if (throwable instanceof com.datastax.oss.driver.api.core.AllNodesFailedException) {
      LOG.info(
          "[{}] {} was detected, initiating failover",
          logPrefix,
          throwable.getClass().toGenericString());
      return true;
    } else if (throwable instanceof com.datastax.oss.driver.api.core.DriverTimeoutException) {
      LOG.info(
          "[{}] {} was detected, initiating failover",
          logPrefix,
          throwable.getClass().toGenericString());
      return true;
    } else if (throwable instanceof com.datastax.oss.driver.api.core.InvalidKeyspaceException) {
      LOG.info(
          "[{}] {} was detected, initiating failover",
          logPrefix,
          throwable.getClass().toGenericString());
      return true;
    } else if (throwable instanceof com.datastax.oss.driver.api.core.NoNodeAvailableException) {
      LOG.info(
          "[{}] {} was detected, initiating failover",
          logPrefix,
          throwable.getClass().toGenericString());
      return true;
    } else {
      return false;
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Adjusts the request to use the profile defined in advanced.failover-profile.profile
   */
  @Override
  public Statement processRequest(Statement<?> request) {
    DriverExecutionProfile driverExecutionProfile =
        Conversions.resolveExecutionProfile(request, context);
    if (!driverExecutionProfile.isDefined(DefaultDriverOption.FAILOVER_POLICY_PROFILE)) {
      throw new IllegalArgumentException("Failover Policy Profile is not defined");
    }
    String failoverProfile =
        driverExecutionProfile.getString(DefaultDriverOption.FAILOVER_POLICY_PROFILE);
    LOG.info(
        "[{}] Failing over request from profile {} to {}",
        logPrefix,
        request.getExecutionProfileName(),
        failoverProfile);
    return request.setExecutionProfile(context.getConfig().getProfile(failoverProfile));
  }
}
