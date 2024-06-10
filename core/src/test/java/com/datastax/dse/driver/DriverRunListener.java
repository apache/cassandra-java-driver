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
package com.datastax.dse.driver;

import static org.assertj.core.api.Assertions.fail;

import org.junit.runner.Description;
import org.junit.runner.notification.RunListener;

/**
 * Common parent of all driver tests, to store common configuration and perform sanity checks.
 *
 * @see "maven-surefire-plugin configuration in pom.xml"
 */
public class DriverRunListener extends RunListener {

  @Override
  public void testFinished(Description description) throws Exception {
    // If a test interrupted the main thread silently, this can make later tests fail. Instead, we
    // fail the test and clear the interrupt status.
    // Note: Thread.interrupted() also clears the flag, which is what we want.
    if (Thread.interrupted()) {
      fail(description.getMethodName() + " interrupted the main thread");
    }
  }
}
