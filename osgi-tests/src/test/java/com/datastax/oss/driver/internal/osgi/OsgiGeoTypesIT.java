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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.osgi.service.MailboxService;
import com.datastax.oss.driver.api.osgi.service.geo.GeoMailboxService;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import com.datastax.oss.driver.internal.osgi.checks.DefaultServiceChecks;
import com.datastax.oss.driver.internal.osgi.checks.GeoServiceChecks;
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
@DseRequirement(min = "5.0", description = "Requires geo types")
public class OsgiGeoTypesIT {

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
        BundleOptions.esriBundles(),
        BundleOptions.testBundles());
  }

  @Test
  public void test_geo_types() throws Exception {
    DefaultServiceChecks.checkService(service);
    assertThat(service).isInstanceOf(GeoMailboxService.class);
    GeoServiceChecks.checkServiceGeo((GeoMailboxService) service);
  }
}
