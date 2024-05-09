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
package com.datastax.oss.driver.internal.osgi.support;

import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirementRule;
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
    if (BackendRequirementRule.meetsDescriptionRequirements(description)) {
      super.run(notifier);
    } else {
      // requirements not met, throw reasoning assumption to skip test
      AssumptionViolatedException e =
          new AssumptionViolatedException(BackendRequirementRule.buildReasonString(description));
      notifier.fireTestAssumptionFailed(new Failure(description, e));
    }
  }
}
