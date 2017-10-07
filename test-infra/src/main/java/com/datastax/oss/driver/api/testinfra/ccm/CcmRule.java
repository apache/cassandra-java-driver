/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
    super(CcmBridge.builder().build());
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

  public static CcmRule getInstance() {
    return INSTANCE;
  }
}
