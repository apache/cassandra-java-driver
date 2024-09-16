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
package com.datastax.oss.driver.api.testinfra.ccm;

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.testinfra.CassandraResourceRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirementRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
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
                    ccmBridge.close();
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
    ccmBridge.close();
  }

  @Override
  public Statement apply(Statement base, Description description) {
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

  public BackendType getDistribution() {
    return CcmBridge.DISTRIBUTION;
  }

  public boolean isDistributionOf(BackendType type) {
    return CcmBridge.isDistributionOf(type);
  }

  public boolean isDistributionOf(BackendType type, CcmBridge.VersionComparator comparator) {
    return CcmBridge.isDistributionOf(type, comparator);
  }

  public Version getDistributionVersion() {
    return CcmBridge.getDistributionVersion();
  }

  public Version getCassandraVersion() {
    return CcmBridge.getCassandraVersion();
  }

  @Override
  public ProtocolVersion getHighestProtocolVersion() {
    if (CcmBridge.getCassandraVersion().compareTo(Version.V2_2_0) >= 0) {
      return DefaultProtocolVersion.V4;
    } else {
      return DefaultProtocolVersion.V3;
    }
  }
}
