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
package com.datastax.dse.driver.osgi;

import static com.datastax.dse.driver.osgi.support.DseBundleOptions.baseOptions;
import static com.datastax.dse.driver.osgi.support.DseBundleOptions.driverCoreBundle;
import static com.datastax.dse.driver.osgi.support.DseBundleOptions.driverDseBundle;
import static com.datastax.dse.driver.osgi.support.DseBundleOptions.driverDseQueryBuilderBundle;
import static com.datastax.dse.driver.osgi.support.DseBundleOptions.driverQueryBuilderBundle;
import static com.datastax.dse.driver.osgi.support.DseBundleOptions.reactiveBundles;
import static com.datastax.oss.driver.osgi.BundleOptions.jacksonBundles;
import static org.ops4j.pax.exam.CoreOptions.options;

import com.datastax.dse.driver.osgi.support.DseOsgiReactiveTests;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.categories.IsolatedTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerMethod;

@RunWith(PaxExam.class)
@ExamReactorStrategy(PerMethod.class)
@Category(IsolatedTests.class)
public class DseOsgiReactiveIT implements DseOsgiReactiveTests {

  @ClassRule
  public static final CustomCcmRule CCM_RULE = CustomCcmRule.builder().withNodes(1).build();

  @Configuration
  public Option[] config() {
    return options(
        driverDseBundle(),
        driverDseQueryBuilderBundle(),
        driverCoreBundle(),
        driverQueryBuilderBundle(),
        baseOptions(),
        jacksonBundles(),
        reactiveBundles());
  }

  @Test
  public void should_connect_and_query_without_reactive() {
    connectAndQuerySimple();
  }

  @Test
  public void should_connect_and_query_with_reactive() {
    connectAndQueryReactive();
  }
}
