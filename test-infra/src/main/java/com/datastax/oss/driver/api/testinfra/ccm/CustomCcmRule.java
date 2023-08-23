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
package com.datastax.oss.driver.api.testinfra.ccm;

import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A rule that creates a ccm cluster that can be used in a test. This should be used if you plan on
 * creating clusters with unique configurations, such as using multiple nodes, authentication, ssl
 * and so on. If you do not plan on doing this at all in your tests, consider using {@link CcmRule}
 * which creates a global single node CCM cluster that may be shared among tests.
 *
 * <p>Note that this rule should be considered mutually exclusive with {@link CcmRule}. Creating
 * instances of these rules can create resource issues.
 */
public class CustomCcmRule extends BaseCcmRule {

  private static final Logger LOG = LoggerFactory.getLogger(CustomCcmRule.class);
  private static final AtomicReference<CustomCcmRule> CURRENT = new AtomicReference<>();

  CustomCcmRule(CcmBridge ccmBridge) {
    super(ccmBridge);
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
            "Error in CustomCcmRule before() method, attempting to clean up leftover state", e);
        try {
          after();
        } catch (Exception e1) {
          LOG.warn("Error cleaning up CustomCcmRule before() failure", e1);
        }
        throw e;
      }
    } else if (CURRENT.get() != this) {
      throw new IllegalStateException(
          "Attempting to use a Ccm rule while another is in use.  This is disallowed");
    }
  }

  @Override
  protected void after() {
    super.after();
    CURRENT.compareAndSet(this, null);
  }

  public CcmBridge getCcmBridge() {
    return ccmBridge;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private final CcmBridge.Builder bridgeBuilder = CcmBridge.builder();

    public Builder withNodes(int... nodes) {
      bridgeBuilder.withNodes(nodes);
      return this;
    }

    public Builder withCassandraConfiguration(String key, Object value) {
      bridgeBuilder.withCassandraConfiguration(key, value);
      return this;
    }

    public Builder withDseConfiguration(String key, Object value) {
      bridgeBuilder.withDseConfiguration(key, value);
      return this;
    }

    public Builder withDseConfiguration(String rawYaml) {
      bridgeBuilder.withDseConfiguration(rawYaml);
      return this;
    }

    public Builder withDseWorkloads(String... workloads) {
      bridgeBuilder.withDseWorkloads(workloads);
      return this;
    }

    public Builder withJvmArgs(String... jvmArgs) {
      bridgeBuilder.withJvmArgs(jvmArgs);
      return this;
    }

    public Builder withCreateOption(String option) {
      bridgeBuilder.withCreateOption(option);
      return this;
    }

    public Builder withSsl() {
      bridgeBuilder.withSsl();
      return this;
    }

    public Builder withSslLocalhostCn() {
      bridgeBuilder.withSslLocalhostCn();
      return this;
    }

    public Builder withSslAuth() {
      bridgeBuilder.withSslAuth();
      return this;
    }

    public CustomCcmRule build() {
      return new CustomCcmRule(bridgeBuilder.build());
    }
  }
}
