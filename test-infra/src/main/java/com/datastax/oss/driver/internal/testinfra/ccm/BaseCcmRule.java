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
package com.datastax.oss.driver.internal.testinfra.ccm;

import com.datastax.oss.driver.api.core.CassandraVersion;
import com.datastax.oss.driver.api.core.CoreProtocolVersion;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.testinfra.CassandraRequirement;
import com.datastax.oss.driver.api.testinfra.CassandraResourceRule;
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import org.junit.AssumptionViolatedException;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public abstract class BaseCcmRule extends CassandraResourceRule {

  protected final CcmBridge ccmBridge;

  private final CassandraVersion cassandraVersion;

  public BaseCcmRule(CcmBridge ccmBridge) {
    this.ccmBridge = ccmBridge;
    this.cassandraVersion = ccmBridge.getCassandraVersion();
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    ccmBridge.remove();
                  } catch (Exception e) {
                    // silently remove as may have already been removed.
                  }
                }));
  }

  @Override
  protected void before() {
    ccmBridge.create();
    ccmBridge.start();
  }

  @Override
  protected void after() {
    ccmBridge.remove();
  }

  @Override
  public Statement apply(Statement base, Description description) {
    CassandraRequirement cassandraRequirement =
        description.getAnnotation(CassandraRequirement.class);

    if (cassandraRequirement != null) {
      // if the configured cassandra cassandraRequirement exceeds the one being used skip this test.
      if (!cassandraRequirement.min().isEmpty()) {
        CassandraVersion minVersion = CassandraVersion.parse(cassandraRequirement.min());
        if (minVersion.compareTo(cassandraVersion) > 0) {
          // Create a statement which simply indicates that the configured cassandra cassandraRequirement is too old for this test.
          return new Statement() {

            @Override
            public void evaluate() throws Throwable {
              throw new AssumptionViolatedException(
                  "Test requires C* "
                      + minVersion
                      + " but "
                      + cassandraVersion
                      + " is configured.  Description: "
                      + cassandraRequirement.description());
            }
          };
        }
      }

      if (!cassandraRequirement.max().isEmpty()) {
        // if the test version exceeds the maximum configured one, fail out.
        CassandraVersion maxVersion = CassandraVersion.parse(cassandraRequirement.max());

        if (maxVersion.compareTo(cassandraVersion) <= 0) {
          return new Statement() {

            @Override
            public void evaluate() throws Throwable {
              throw new AssumptionViolatedException(
                  "Test requires C* less than "
                      + maxVersion
                      + " but "
                      + cassandraVersion
                      + " is configured.  Description: "
                      + cassandraRequirement.description());
            }
          };
        }
      }
    }
    return super.apply(base, description);
  }

  public CassandraVersion getCassandraVersion() {
    return cassandraVersion;
  }

  @Override
  public ProtocolVersion getHighestProtocolVersion() {
    if (cassandraVersion.compareTo(CassandraVersion.V2_2_0) >= 0) {
      return CoreProtocolVersion.V4;
    } else {
      return CoreProtocolVersion.V3;
    }
  }
}
