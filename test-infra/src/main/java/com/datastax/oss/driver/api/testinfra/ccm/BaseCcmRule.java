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

/*
 * Copyright (C) 2022 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.oss.driver.api.testinfra.ccm;

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.testinfra.CassandraResourceRule;
import com.datastax.oss.driver.api.testinfra.CassandraSkip;
import com.datastax.oss.driver.api.testinfra.ScyllaRequirement;
import com.datastax.oss.driver.api.testinfra.ScyllaSkip;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirementRule;
import java.util.Objects;
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
                dse ? "DSE" : (CcmBridge.SCYLLA_ENABLEMENT ? "SCYLLA" : "C*"),
                requirement,
                dse
                    ? ccmBridge.getDseVersion().orElse(null)
                    : (CcmBridge.SCYLLA_ENABLEMENT
                        ? ccmBridge.getScyllaVersion().orElse(null)
                        : ccmBridge.getCassandraVersion()),
                description));
      }
    };
  }

  @Override
  public Statement apply(Statement base, Description description) {

    // Legacy skipping:

    // Scylla-specific annotations
    ScyllaSkip scyllaSkip = description.getAnnotation(ScyllaSkip.class);
    if (scyllaSkip != null) {
      if (CcmBridge.SCYLLA_ENABLEMENT) {
        return new Statement() {

          @Override
          public void evaluate() {
            throw new AssumptionViolatedException(
                String.format(
                    "Test skipped when running with Scylla.  Description: %s", description));
          }
        };
      }
    }

    CassandraSkip cassandraSkip = description.getAnnotation(CassandraSkip.class);
    if (cassandraSkip != null) {
      if (!CcmBridge.SCYLLA_ENABLEMENT) {
        return new Statement() {

          @Override
          public void evaluate() {
            throw new AssumptionViolatedException(
                String.format(
                    "Test skipped when running with Cassandra.  Description: %s", description));
          }
        };
      }
    }

    ScyllaRequirement scyllaRequirement = description.getAnnotation(ScyllaRequirement.class);
    if (scyllaRequirement != null) {
      Optional<Version> scyllaVersionOption = ccmBridge.getScyllaVersion();
      if (!scyllaVersionOption.isPresent()) {
        return new Statement() {
          @Override
          public void evaluate() {
            throw new AssumptionViolatedException(
                "Test has Scylla version requirement, but CCMBridge is not configured for Scylla.");
          }
        };
      }
      Version scyllaVersion = scyllaVersionOption.get();
      if (CcmBridge.SCYLLA_ENTERPRISE) {
        if (!scyllaRequirement.minEnterprise().isEmpty()) {
          Version minVersion =
              Objects.requireNonNull(Version.parse(scyllaRequirement.minEnterprise()));
          if (minVersion.compareTo(scyllaVersion) > 0) {
            return buildErrorStatement(minVersion, scyllaRequirement.description(), false, false);
          }
        }
        if (!scyllaRequirement.maxEnterprise().isEmpty()) {
          Version maxVersion =
              Objects.requireNonNull(Version.parse(scyllaRequirement.maxEnterprise()));
          if (maxVersion.compareTo(scyllaVersion) <= 0) {
            return buildErrorStatement(maxVersion, scyllaRequirement.description(), true, false);
          }
        }
      } else {
        if (!scyllaRequirement.minOSS().isEmpty()) {
          Version minVersion = Objects.requireNonNull(Version.parse(scyllaRequirement.minOSS()));
          if (minVersion.compareTo(scyllaVersion) > 0) {
            return buildErrorStatement(minVersion, scyllaRequirement.description(), false, false);
          }
        }
        if (!scyllaRequirement.maxOSS().isEmpty()) {
          Version maxVersion = Objects.requireNonNull(Version.parse(scyllaRequirement.maxOSS()));
          if (maxVersion.compareTo(CcmBridge.VERSION) <= 0) {
            return buildErrorStatement(maxVersion, scyllaRequirement.description(), true, false);
          }
        }
      }
    }

    if (BackendRequirementRule.meetsDescriptionRequirements(description)) {
      return super.apply(base, description);
    } else {
      // requirements not met, throw reasoning assumption to skip test
      return new Statement() {
        @Override
        public void evaluate() {
          throw new AssumptionViolatedException(
              BackendRequirementRule.buildReasonString(description));
        }
      };
    }
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
