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

import com.datastax.oss.driver.api.testinfra.astra.AstraRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
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
 *
 * <p>When {@code ccm.distribution} system property is set to {@code ASTRA}, this rule will delegate
 * to {@link AstraRule} instead of creating a CCM cluster.
 */
public class CcmRule extends BaseCcmRule {

  private static volatile CcmRule CCM_INSTANCE;
  private static final BackendType DISTRIBUTION =
      BackendType.valueOf(
          System.getProperty("ccm.distribution", BackendType.CASSANDRA.name()).toUpperCase());

  private volatile boolean started = false;

  protected CcmRule() {
    super(configureCcmBridge(CcmBridge.builder()).build());
  }

  /**
   * Protected constructor for subclasses (like AstraRule) that want to provide their own bridge
   * implementation.
   */
  protected CcmRule(CcmBridge bridge) {
    super(bridge);
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

  /**
   * Returns a singleton instance of a CCM rule.
   *
   * <p>The actual implementation returned depends on the {@code ccm.distribution} system property:
   *
   * <ul>
   *   <li>If set to {@code ASTRA}, returns {@link AstraRule#getInstance()} (which extends CcmRule)
   *   <li>Otherwise, returns a {@link CcmRule} instance
   * </ul>
   *
   * @return a singleton CCM rule
   */
  public static CcmRule getInstance() {
    if (DISTRIBUTION == BackendType.ASTRA) {
      return AstraRule.getInstance();
    }
    // Lazy initialization to avoid creating CcmBridge when using Astra
    if (CCM_INSTANCE == null) {
      synchronized (CcmRule.class) {
        if (CCM_INSTANCE == null) {
          CCM_INSTANCE = new CcmRule();
        }
      }
    }
    return CCM_INSTANCE;
  }
}
