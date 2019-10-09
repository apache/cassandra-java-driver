/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.insights;

import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.SSL_ENGINE_FACTORY_CLASS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.SSL_HOSTNAME_VALIDATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.dse.driver.internal.core.context.DseDriverContext;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.Collections;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class ConfigAntiPatternsFinderTest {

  private static final ImmutableMap<String, String> SSL_ANTI_PATTERN =
      ImmutableMap.of(
          "sslWithoutCertValidation",
          "Client-to-node encryption is enabled but server certificate validation is disabled");

  @Test
  @UseDataProvider("sslConfigProvider")
  public void should_find_ssl_anti_pattern(
      boolean sslEngineFactoryClassDefined,
      boolean hostnameValidation,
      Map<String, String> expected) {
    // given
    DseDriverContext dseDriverContext =
        mockDefaultProfile(sslEngineFactoryClassDefined, hostnameValidation);

    // when
    Map<String, String> antiPatterns =
        new ConfigAntiPatternsFinder().findAntiPatterns(dseDriverContext);

    // then
    assertThat(antiPatterns).isEqualTo(expected);
  }

  private DseDriverContext mockDefaultProfile(
      boolean sslEngineFactoryClassDefined, boolean hostnameValidation) {
    DseDriverContext dseDriverContext = mock(DseDriverContext.class);
    DriverConfig driverConfig = mock(DriverConfig.class);
    when(dseDriverContext.getConfig()).thenReturn(driverConfig);
    DriverExecutionProfile profile = mock(DriverExecutionProfile.class);
    when(profile.isDefined(SSL_ENGINE_FACTORY_CLASS)).thenReturn(sslEngineFactoryClassDefined);
    when(profile.getBoolean(SSL_HOSTNAME_VALIDATION, false)).thenReturn(hostnameValidation);
    when(driverConfig.getDefaultProfile()).thenReturn(profile);
    return dseDriverContext;
  }

  @DataProvider
  public static Object[][] sslConfigProvider() {
    return new Object[][] {
      {true, true, Collections.emptyMap()},
      {true, false, SSL_ANTI_PATTERN},
      {false, false, Collections.emptyMap()},
      {false, true, Collections.emptyMap()}
    };
  }
}
