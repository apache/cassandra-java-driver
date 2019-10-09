/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.osgi;

import static com.datastax.dse.driver.osgi.support.DseBundleOptions.baseOptions;
import static com.datastax.dse.driver.osgi.support.DseBundleOptions.driverCoreBundle;
import static com.datastax.dse.driver.osgi.support.DseBundleOptions.driverDseBundle;
import static com.datastax.dse.driver.osgi.support.DseBundleOptions.driverDseQueryBuilderBundle;
import static com.datastax.dse.driver.osgi.support.DseBundleOptions.driverQueryBuilderBundle;
import static com.datastax.oss.driver.osgi.BundleOptions.jacksonBundles;
import static org.ops4j.pax.exam.CoreOptions.options;

import com.datastax.dse.driver.api.testinfra.DseSessionBuilderInstantiator;
import com.datastax.dse.driver.osgi.support.DseOsgiSimpleTests;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.loadbalancing.SortingLoadBalancingPolicy;
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

/**
 * Test that uses a policy from a separate bundle from the core driver to ensure that the driver is
 * able to load that policy via Reflection. To support this, the driver uses <code>
 * DynamicImport-Package: *</code>.
 */
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerMethod.class)
@Category(IsolatedTests.class)
public class DseOsgiCustomLoadBalancingPolicyIT implements DseOsgiSimpleTests {

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
        jacksonBundles());
  }

  @Override
  public ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder() {
    return DseSessionBuilderInstantiator.configLoaderBuilder()
        .withClass(
            DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, SortingLoadBalancingPolicy.class);
  }

  @Test
  public void should_connect_and_query_with_custom_lbp() {
    connectAndQuerySimple();
  }
}
