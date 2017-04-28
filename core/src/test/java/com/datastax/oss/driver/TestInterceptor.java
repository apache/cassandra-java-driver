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
package com.datastax.oss.driver;

import org.testng.IHookCallBack;
import org.testng.IHookable;
import org.testng.ITestResult;

/**
 * Intercepts the execution of each test, in order to perform additional tests.
 *
 * @see "src/test/resources/META-INF/services/org.testng.ITestNGListener"
 */
public class TestInterceptor implements IHookable {

  @Override
  public void run(IHookCallBack callback, ITestResult result) {

    // If a test interrupts the main thread silently, this can make later tests fail. Instead, we
    // fail the test and clear the interrupt status.
    boolean wasInterrupted = Thread.currentThread().isInterrupted();

    callback.runTestMethod(result);

    // Note: Thread.interrupted() also clears the flag, which is what we want.
    if (!wasInterrupted && Thread.interrupted()) {
      result.setStatus(ITestResult.FAILURE);
      result.setThrowable(new IllegalStateException("The test interrupted the main thread"));
    }
  }
}
