/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver;

import com.datastax.dse.driver.api.core.DseProtocolVersion;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.tngtech.java.junit.dataprovider.DataProvider;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Arrays;
import java.util.stream.Stream;

public class DseTestDataProviders {

  @DataProvider
  public static Object[][] allDseProtocolVersions() {
    return concat(DseProtocolVersion.values());
  }

  @DataProvider
  public static Object[][] allOssProtocolVersions() {
    return concat(DefaultProtocolVersion.values());
  }

  @DataProvider
  public static Object[][] allDseAndOssProtocolVersions() {
    return concat(DefaultProtocolVersion.values(), DseProtocolVersion.values());
  }

  @NonNull
  private static Object[][] concat(Object[]... values) {
    return Stream.of(values)
        .flatMap(Arrays::stream)
        .map(o -> new Object[] {o})
        .toArray(Object[][]::new);
  }
}
