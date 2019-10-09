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
import static com.datastax.oss.driver.osgi.BundleOptions.lz4Bundle;
import static org.ops4j.pax.exam.CoreOptions.options;

import com.datastax.dse.driver.api.testinfra.DseSessionBuilderInstantiator;
import com.datastax.dse.driver.osgi.support.DseOsgiSimpleTests;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
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
public class DseOsgiLz4IT implements DseOsgiSimpleTests {

  @ClassRule
  public static final CustomCcmRule CCM_RULE = CustomCcmRule.builder().withNodes(1).build();

  @Configuration
  public Option[] config() {
    return options(
        lz4Bundle(),
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
        .withString(DefaultDriverOption.PROTOCOL_COMPRESSION, "lz4");
  }

  @Test
  public void should_connect_and_query_with_lz4_compression() {
    connectAndQuerySimple();
  }
}
