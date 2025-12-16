/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.api.testinfra.astra;

import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A rule that creates an Astra database that can be used in a test. This should be used if you plan
 * on creating databases with unique configurations, such as different cloud providers, regions, or
 * keyspaces. If you do not plan on doing this at all in your tests, consider using {@link
 * AstraRule} which creates a global Astra database that may be shared among tests.
 *
 * <p>Note that this rule should be considered mutually exclusive with {@link AstraRule}. Creating
 * instances of these rules can create resource issues.
 */
public class CustomAstraRule extends BaseAstraRule {

  private static final Logger LOG = LoggerFactory.getLogger(CustomAstraRule.class);
  private static final AtomicReference<CustomAstraRule> CURRENT = new AtomicReference<>();

  CustomAstraRule(AstraBridge astraBridge) {
    super(astraBridge);
  }

  @Override
  protected void before() {
    if (CURRENT.get() == null && CURRENT.compareAndSet(null, this)) {
      try {
        super.before();
      } catch (Exception e) {
        // ExternalResource will not call after() when before() throws an exception
        // Let's try and clean up and release the lock we have in CURRENT
        LOG.warn(
            "Error in CustomAstraRule before() method, attempting to clean up leftover state", e);
        try {
          after();
        } catch (Exception e1) {
          LOG.warn("Error cleaning up CustomAstraRule before() failure", e1);
          e.addSuppressed(e1);
        }
        throw e;
      }
    } else if (CURRENT.get() != this) {
      throw new IllegalStateException(
          "Attempting to use an Astra rule while another is in use. This is disallowed");
    }
  }

  @Override
  protected void after() {
    try {
      super.after();
    } finally {
      CURRENT.compareAndSet(this, null);
    }
  }

  @Override
  public AstraBridge getAstraBridge() {
    return astraBridge;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private final AstraBridge.Builder bridgeBuilder = AstraBridge.builder();

    public Builder withDatabaseName(String databaseName) {
      bridgeBuilder.withDatabaseName(databaseName);
      return this;
    }

    public Builder withKeyspace(String keyspace) {
      bridgeBuilder.withKeyspace(keyspace);
      return this;
    }

    public Builder withCloudProvider(String cloudProvider) {
      bridgeBuilder.withCloudProvider(cloudProvider);
      return this;
    }

    public Builder withRegion(String region) {
      bridgeBuilder.withRegion(region);
      return this;
    }

    public CustomAstraRule build() {
      return new CustomAstraRule(bridgeBuilder.build());
    }
  }
}
