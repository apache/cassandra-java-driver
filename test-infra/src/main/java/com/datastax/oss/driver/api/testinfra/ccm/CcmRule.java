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

import com.datastax.oss.driver.categories.ParallelizableTests;
import java.lang.reflect.Method;
import org.junit.AssumptionViolatedException;
import org.junit.experimental.categories.Category;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A rule that creates a globally shared single node Ccm cluster that is only shut down after the
 * JVM exists.
 *
 * <p>Note that this rule should be considered mutually exclusive with {@link CustomCcmRule}.
 * Creating instances of these rules can create resource issues.
 */
public class CcmRule extends BaseCcmRule {

  private static final CcmRule INSTANCE = new CcmRule();

  private volatile boolean started = false;

  private CcmRule() {
    super(configureCcmBridge(CcmBridge.builder()).build());
  }

  public static CcmBridge.Builder configureCcmBridge(CcmBridge.Builder builder) {
    Logger logger = LoggerFactory.getLogger(CcmRule.class);
    String customizerClass =
        System.getProperty(
            "ccmrule.bridgecustomizer",
            "com.datastax.oss.driver.api.testinfra.ccm.DefaultCcmBridgeBuilderCustomizer");
    try {
      Class<?> clazz = Class.forName(customizerClass);
      Method method = clazz.getMethod("configureBuilder", CcmBridge.Builder.class);
      return (CcmBridge.Builder) method.invoke(null, builder);
    } catch (Exception e) {
      logger.warn(
          "Could not find CcmRule customizer {}, will use the default CcmBridge.",
          customizerClass,
          e);
      return builder;
    }
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
                  CcmRule.class.getSimpleName(),
                  ParallelizableTests.class.getSimpleName(),
                  description));
        }
      };
    }

    return super.apply(base, description);
  }

  public static CcmRule getInstance() {
    return INSTANCE;
  }
}
