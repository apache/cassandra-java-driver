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
import com.datastax.oss.driver.api.testinfra.CassandraResourceRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.requirement.VersionRequirement;
import java.util.Collection;
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

  @Override
  public Statement apply(Statement base, Description description) {
    BackendType backend =
        ccmBridge.getDseVersion().isPresent() ? BackendType.DSE : BackendType.CASSANDRA;
    Version version = ccmBridge.getDseVersion().orElseGet(ccmBridge::getCassandraVersion);

    Collection<VersionRequirement> requirements = VersionRequirement.fromAnnotations(description);

    if (VersionRequirement.meetsAny(requirements, backend, version)) {
      return super.apply(base, description);
    } else {
      // requirements not met, throw reasoning assumption to skip test
      return new Statement() {
        @Override
        public void evaluate() {
          throw new AssumptionViolatedException(
              VersionRequirement.buildReasonString(requirements, backend, version));
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
