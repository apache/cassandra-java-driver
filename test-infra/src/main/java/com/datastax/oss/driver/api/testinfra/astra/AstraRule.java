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

import com.datastax.oss.driver.categories.ParallelizableTests;
import org.junit.AssumptionViolatedException;
import org.junit.experimental.categories.Category;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * A rule that creates a globally shared Astra database that is only terminated after the JVM exits.
 *
 * <p>Note that this rule should be considered mutually exclusive with {@link CustomAstraRule}.
 * Creating instances of these rules can create resource issues.
 */
public class AstraRule extends BaseAstraRule {

  private static volatile AstraRule INSTANCE;

  private volatile boolean started = false;

  private AstraRule() {
    super(AstraBridge.builder().build());
  }

  @Override
  protected synchronized void before() {
    if (!started) {
      // synchronize before so blocks on other before() call waiting to finish.
      super.before();
      started = true;
    }
  }

  @Override
  protected void after() {
    // override after so we don't remove when done.
  }

  @Override
  public Statement apply(Statement base, Description description) {

    Category categoryAnnotation = description.getTestClass().getAnnotation(Category.class);
    if (categoryAnnotation == null
        || categoryAnnotation.value().length != 1
        || categoryAnnotation.value()[0] != ParallelizableTests.class) {
      return new Statement() {
        @Override
        public void evaluate() {
          throw new AssumptionViolatedException(
              String.format(
                  "Tests using %s must be annotated with `@Category(%s.class)`. Description: %s",
                  AstraRule.class.getSimpleName(),
                  ParallelizableTests.class.getSimpleName(),
                  description));
        }
      };
    }

    return super.apply(base, description);
  }

  public static AstraRule getInstance() {
    // Lazy initialization to avoid creating AstraBridge when not using Astra
    if (INSTANCE == null) {
      synchronized (AstraRule.class) {
        if (INSTANCE == null) {
          INSTANCE = new AstraRule();
        }
      }
    }
    return INSTANCE;
  }
}
