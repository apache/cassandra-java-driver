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

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.testinfra.CassandraRequirement;
import com.datastax.oss.driver.api.testinfra.CassandraResourceRule;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import java.util.Optional;
import org.junit.AssumptionViolatedException;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public abstract class BaseCcmRule extends CassandraResourceRule {

  protected final CcmBridge ccmBridge;

  BaseCcmRule(CcmBridge ccmBridge) {
    this.ccmBridge = ccmBridge;
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

  private Statement buildErrorStatement(
      Version requirement, String description, boolean lessThan, boolean dse) {
    return new Statement() {

      @Override
      public void evaluate() {
        throw new AssumptionViolatedException(
            String.format(
                "Test requires %s %s %s but %s is configured.  Description: %s",
                lessThan ? "less than" : "at least",
                dse ? "DSE" : "C*",
                requirement,
                dse ? ccmBridge.getDseVersion().orElse(null) : ccmBridge.getCassandraVersion(),
                description));
      }
    };
  }

  @Override
  public Statement apply(Statement base, Description description) {
    // If test is annotated with CassandraRequirement or DseRequirement, ensure configured CCM
    // cluster meets those requirements.
    CassandraRequirement cassandraRequirement =
        description.getAnnotation(CassandraRequirement.class);

    if (cassandraRequirement != null) {
      // if the configured cassandra cassandraRequirement exceeds the one being used skip this test.
      if (!cassandraRequirement.min().isEmpty()) {
        Version minVersion = Version.parse(cassandraRequirement.min());
        if (minVersion.compareTo(ccmBridge.getCassandraVersion()) > 0) {
          return buildErrorStatement(minVersion, cassandraRequirement.description(), false, false);
        }
      }

      if (!cassandraRequirement.max().isEmpty()) {
        // if the test version exceeds the maximum configured one, fail out.
        Version maxVersion = Version.parse(cassandraRequirement.max());

        if (maxVersion.compareTo(ccmBridge.getCassandraVersion()) <= 0) {
          return buildErrorStatement(maxVersion, cassandraRequirement.description(), true, false);
        }
      }
    }

    DseRequirement dseRequirement = description.getAnnotation(DseRequirement.class);
    if (dseRequirement != null) {
      Optional<Version> dseVersionOption = ccmBridge.getDseVersion();
      if (!dseVersionOption.isPresent()) {
        return new Statement() {

          @Override
          public void evaluate() {
            throw new AssumptionViolatedException("Test Requires DSE but C* is configured.");
          }
        };
      } else {
        Version dseVersion = dseVersionOption.get();
        if (!dseRequirement.min().isEmpty()) {
          Version minVersion = Version.parse(dseRequirement.min());
          if (minVersion.compareTo(dseVersion) > 0) {
            return buildErrorStatement(minVersion, dseRequirement.description(), false, true);
          }
        }

        if (!dseRequirement.max().isEmpty()) {
          Version maxVersion = Version.parse(dseRequirement.max());

          if (maxVersion.compareTo(dseVersion) <= 0) {
            return buildErrorStatement(maxVersion, dseRequirement.description(), true, true);
          }
        }
      }
    }
    return super.apply(base, description);
  }

  public Version getCassandraVersion() {
    return ccmBridge.getCassandraVersion();
  }

  public Optional<Version> getDseVersion() {
    return ccmBridge.getDseVersion();
  }

  @Override
  public ProtocolVersion getHighestProtocolVersion() {
    if (ccmBridge.getCassandraVersion().compareTo(Version.V2_2_0) >= 0) {
      return DefaultProtocolVersion.V4;
    } else {
      return DefaultProtocolVersion.V3;
    }
  }
}
