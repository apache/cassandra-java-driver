/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.insights;

import static org.assertj.core.api.Assertions.assertThat;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class AddressFormatterTest {

  @Test
  @UseDataProvider("addressesProvider")
  public void should_format_addresses(Object address, String expected) {
    // when
    String result = AddressFormatter.nullSafeToString(address);

    // then
    assertThat(result).isEqualTo(expected);
  }

  @DataProvider
  public static Object[][] addressesProvider() throws UnknownHostException {
    return new Object[][] {
      {new InetSocketAddress(8888), "0.0.0.0:8888"},
      {new InetSocketAddress("127.0.0.1", 8888), "127.0.0.1:8888"},
      {InetSocketAddress.createUnresolved("127.0.0.2", 8080), "127.0.0.2:8080"},
      {InetAddress.getByName("127.0.0.1"), "127.0.0.1"},
    };
  }
}
