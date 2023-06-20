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
package com.datastax.oss.driver.internal.osgi;

import com.datastax.oss.driver.api.osgi.service.MailboxService;
import com.datastax.oss.driver.api.testinfra.loadbalancing.SortingLoadBalancingPolicy;
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

/**
 * Test that uses a policy from a separate bundle from the core driver to ensure that the driver is
 * able to load that policy via Reflection. To support this, the driver uses <code>
 * DynamicImport-Package: *</code>.
 */
@RunWith(CcmPaxExam.class)
@ExamReactorStrategy(CcmExamReactorFactory.class)
public class OsgiCustomLoadBalancingPolicyIT {

  @Inject MailboxService service;

  @Configuration
  public Option[] config() {
    return CoreOptions.options(
        BundleOptions.applicationBundle(),
        BundleOptions.driverCoreBundle(),
        BundleOptions.driverQueryBuilderBundle(),
        BundleOptions.driverMapperRuntimeBundle(),
        BundleOptions.commonBundles(),
        BundleOptions.nettyBundles(),
        BundleOptions.jacksonBundles(),
        BundleOptions.testBundles(),
        CoreOptions.systemProperty("cassandra.lbp")
            // This LBP resides in test-infra bundle and will be loaded the driver
            // class loader, thanks to the "Dynamic-Import:*" directive
            .value(SortingLoadBalancingPolicy.class.getName()));
  }

  @Test
  public void test_custom_lbp() throws Exception {
    DefaultServiceChecks.checkService(service);
  }
}
