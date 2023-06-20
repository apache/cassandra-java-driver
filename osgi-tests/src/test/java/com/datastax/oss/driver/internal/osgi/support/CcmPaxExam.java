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
import com.datastax.oss.driver.api.testinfra.CassandraRequirement;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import java.util.Objects;
import java.util.Optional;
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
    CassandraRequirement cassandraRequirement =
        description.getAnnotation(CassandraRequirement.class);
    if (cassandraRequirement != null) {
      if (!cassandraRequirement.min().isEmpty()) {
        Version minVersion = Objects.requireNonNull(Version.parse(cassandraRequirement.min()));
        if (minVersion.compareTo(CCM_BRIDGE.getCassandraVersion()) > 0) {
          fireRequirementsNotMet(notifier, description, cassandraRequirement.min(), false, false);
          return;
        }
      }
      if (!cassandraRequirement.max().isEmpty()) {
        Version maxVersion = Objects.requireNonNull(Version.parse(cassandraRequirement.max()));
        if (maxVersion.compareTo(CCM_BRIDGE.getCassandraVersion()) <= 0) {
          fireRequirementsNotMet(notifier, description, cassandraRequirement.max(), true, false);
          return;
        }
      }
    }
    DseRequirement dseRequirement = description.getAnnotation(DseRequirement.class);
    if (dseRequirement != null) {
      Optional<Version> dseVersionOption = CCM_BRIDGE.getDseVersion();
      if (!dseVersionOption.isPresent()) {
        notifier.fireTestAssumptionFailed(
            new Failure(
                description,
                new AssumptionViolatedException("Test Requires DSE but C* is configured.")));
        return;
      } else {
        Version dseVersion = dseVersionOption.get();
        if (!dseRequirement.min().isEmpty()) {
          Version minVersion = Objects.requireNonNull(Version.parse(dseRequirement.min()));
          if (minVersion.compareTo(dseVersion) > 0) {
            fireRequirementsNotMet(notifier, description, dseRequirement.min(), false, true);
            return;
          }
        }
        if (!dseRequirement.max().isEmpty()) {
          Version maxVersion = Objects.requireNonNull(Version.parse(dseRequirement.max()));
          if (maxVersion.compareTo(dseVersion) <= 0) {
            fireRequirementsNotMet(notifier, description, dseRequirement.min(), true, true);
            return;
          }
        }
      }
    }
    super.run(notifier);
  }

  private void fireRequirementsNotMet(
      RunNotifier notifier,
      Description description,
      String requirement,
      boolean lessThan,
      boolean dse) {
    AssumptionViolatedException e =
        new AssumptionViolatedException(
            String.format(
                "Test requires %s %s %s but %s is configured.  Description: %s",
                lessThan ? "less than" : "at least",
                dse ? "DSE" : "C*",
                requirement,
                dse ? CCM_BRIDGE.getDseVersion().orElse(null) : CCM_BRIDGE.getCassandraVersion(),
                description));
    notifier.fireTestAssumptionFailed(new Failure(description, e));
  }
}
