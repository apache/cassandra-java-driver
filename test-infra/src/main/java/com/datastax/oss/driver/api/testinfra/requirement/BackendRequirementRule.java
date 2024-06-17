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
package com.datastax.oss.driver.api.testinfra.requirement;

import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import org.junit.AssumptionViolatedException;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class BackendRequirementRule extends ExternalResource {
  @Override
  public Statement apply(Statement base, Description description) {
    if (meetsDescriptionRequirements(description)) {
      return super.apply(base, description);
    } else {
      // requirements not met, throw reasoning assumption to skip test
      return new Statement() {
        @Override
        public void evaluate() {
          throw new AssumptionViolatedException(buildReasonString(description));
        }
      };
    }
  }

  protected static BackendType getBackendType() {
    return CcmBridge.DSE_ENABLEMENT ? BackendType.DSE : BackendType.CASSANDRA;
  }

  protected static Version getVersion() {
    return CcmBridge.VERSION;
  }

  public static boolean meetsDescriptionRequirements(Description description) {
    return VersionRequirement.meetsAny(
        VersionRequirement.fromAnnotations(description), getBackendType(), getVersion());
  }

  /* Note, duplicating annotation processing from #meetsDescriptionRequirements */
  public static String buildReasonString(Description description) {
    return VersionRequirement.buildReasonString(
        VersionRequirement.fromAnnotations(description), getBackendType(), getVersion());
  }
}
