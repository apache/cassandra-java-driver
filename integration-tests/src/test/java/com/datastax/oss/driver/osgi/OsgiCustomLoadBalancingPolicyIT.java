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
package com.datastax.oss.driver.osgi;

import static org.ops4j.pax.exam.CoreOptions.options;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.loadbalancing.SortingLoadBalancingPolicy;
import com.datastax.oss.driver.categories.IsolatedTests;
import com.datastax.oss.driver.osgi.support.BundleOptions;
import com.datastax.oss.driver.osgi.support.OsgiSimpleTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerMethod;

/**
 * Test that uses a policy from a separate bundle from the core driver to ensure that the driver is
 * able to load that policy via Reflection. To support this, the driver uses <code>
 * DynamicImport-Package: *</code>.
 */
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerMethod.class)
@Category(IsolatedTests.class)
public class OsgiCustomLoadBalancingPolicyIT implements OsgiSimpleTests {

  @ClassRule
  public static final CustomCcmRule CCM_RULE = CustomCcmRule.builder().withNodes(1).build();

  @Configuration
  public Option[] config() {
    return options(
        BundleOptions.driverCoreBundle(),
        BundleOptions.driverQueryBuilderBundle(),
        BundleOptions.baseOptions(),
        BundleOptions.jacksonBundles());
  }

  @Override
  public ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder() {
    return DriverConfigLoader.programmaticBuilder()
        .withClass(
            DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, SortingLoadBalancingPolicy.class);
  }

  @Test
  public void should_connect_and_query_with_custom_lbp() {
    connectAndQuerySimple();
  }
}
