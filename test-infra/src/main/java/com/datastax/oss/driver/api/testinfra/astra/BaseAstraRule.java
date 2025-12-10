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

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.testinfra.CassandraResourceRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirementRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import java.io.File;
import java.util.Collections;
import java.util.Set;
import org.junit.AssumptionViolatedException;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public abstract class BaseAstraRule extends CassandraResourceRule {

  protected final AstraBridge astraBridge;

  BaseAstraRule(AstraBridge astraBridge) {
    this.astraBridge = astraBridge;
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    astraBridge.close();
                  } catch (Exception e) {
                    // silently remove as may have already been removed.
                  }
                }));
  }

  @Override
  protected void before() {
    astraBridge.create();
    astraBridge.start();
  }

  @Override
  protected void after() {
    astraBridge.close();
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
    return AstraBridge.DISTRIBUTION;
  }

  public boolean isDistributionOf(BackendType type) {
    return AstraBridge.isDistributionOf(type);
  }

  public Version getDistributionVersion() {
    return AstraBridge.getDistributionVersion();
  }

  public Version getCassandraVersion() {
    return AstraBridge.getCassandraVersion();
  }

  @Override
  public ProtocolVersion getHighestProtocolVersion() {
    // Astra supports protocol version V4
    return DefaultProtocolVersion.V4;
  }

  @Override
  public Set<EndPoint> getContactPoints() {
    // Astra uses Secure Connect Bundle instead of contact points
    return Collections.emptySet();
  }

  /**
   * Returns the Secure Connect Bundle file for connecting to Astra.
   *
   * @return the Secure Connect Bundle file
   */
  public File getSecureConnectBundle() {
    return astraBridge.getSecureConnectBundle();
  }

  public AstraBridge getAstraBridge() {
    return astraBridge;
  }
}
