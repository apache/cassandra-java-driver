/*
 * Copyright (C) 2017-2017 DataStax Inc.
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

import com.datastax.oss.driver.api.core.CassandraVersion;
import com.datastax.oss.driver.internal.testinfra.ccm.BaseCcmRule;
import java.util.concurrent.atomic.AtomicReference;

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

  private static AtomicReference<CustomCcmRule> current = new AtomicReference<>();

  CustomCcmRule(CcmBridge ccmBridge) {
    super(ccmBridge);
  }

  @Override
  protected void before() {
    if (current.get() == null && current.compareAndSet(null, this)) {
      super.before();
    } else if (current.get() != this) {
      throw new IllegalStateException(
          "Attempting to use a Ccm rule while another is in use.  This is disallowed");
    }
  }

  @Override
  protected void after() {
    super.after();
    current.compareAndSet(this, null);
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

    public Builder withJvmArgs(String... jvmArgs) {
      bridgeBuilder.withJvmArgs(jvmArgs);
      return this;
    }

    public Builder withCassandraVersion(CassandraVersion cassandraVersion) {
      bridgeBuilder.withCassandraVersion(cassandraVersion);
      return this;
    }

    public Builder withSsl() {
      bridgeBuilder.withSsl();
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
