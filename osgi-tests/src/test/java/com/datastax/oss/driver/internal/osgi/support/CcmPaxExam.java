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
package com.datastax.oss.driver.internal.osgi.support;

import static com.datastax.oss.driver.internal.osgi.support.CcmStagedReactor.CCM_BRIDGE;

import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.requirement.VersionRequirement;
import java.util.Collection;
import org.junit.AssumptionViolatedException;
import org.junit.runner.Description;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.model.InitializationError;
import org.ops4j.pax.exam.junit.PaxExam;

public class CcmPaxExam extends PaxExam {

  public CcmPaxExam(Class<?> klass) throws InitializationError {
    super(klass);
  }

  @Override
  public void run(RunNotifier notifier) {
    Description description = getDescription();
    BackendType backend =
        CCM_BRIDGE.getDseVersion().isPresent() ? BackendType.DSE : BackendType.CASSANDRA;
    Version version = CCM_BRIDGE.getDseVersion().orElseGet(CCM_BRIDGE::getCassandraVersion);

    Collection<VersionRequirement> requirements =
        VersionRequirement.fromAnnotations(getDescription());
    if (VersionRequirement.meetsAny(requirements, backend, version)) {
      super.run(notifier);
    } else {
      // requirements not met, throw reasoning assumption to skip test
      AssumptionViolatedException e =
          new AssumptionViolatedException(
              VersionRequirement.buildReasonString(requirements, backend, version));
      notifier.fireTestAssumptionFailed(new Failure(description, e));
    }
  }
}
