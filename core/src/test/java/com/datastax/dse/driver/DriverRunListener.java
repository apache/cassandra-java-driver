/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
