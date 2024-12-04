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
package com.datastax.oss.driver.internal.osgi;

import com.datastax.oss.driver.api.osgi.service.TweetService;
import com.datastax.oss.driver.internal.osgi.checks.DefaultServiceChecks;
import com.datastax.oss.driver.internal.osgi.support.BundleOptions;
import com.datastax.oss.driver.internal.osgi.support.CcmExamReactorFactory;
import com.datastax.oss.driver.internal.osgi.support.CcmPaxExam;
import javax.inject.Inject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.CoreOptions;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;

@RunWith(CcmPaxExam.class)
@ExamReactorStrategy(CcmExamReactorFactory.class)
public class OsgiCoreShadedIT {
  @Inject TweetService service;

  @Configuration
  public Option[] config() {
    return CoreOptions.options(
        BundleOptions.applicationCoreBundle(),
        BundleOptions.driverCoreShadedBundle(),
        // Guava is required by query builder, runtime mapper and test infra
        BundleOptions.commonBundles(),
        // Netty and Jackson are shaded
        // If this tests breaks due to Guava bundle, it may be the case
        // that test infrastructure uses Guava
        BundleOptions.testBundles());
  }

  @Test
  public void test_shaded() throws Exception {
    DefaultServiceChecks.checkService(service);
  }
}
