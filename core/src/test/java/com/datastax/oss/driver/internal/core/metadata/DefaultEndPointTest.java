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
package com.datastax.oss.driver.internal.core.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class DefaultEndPointTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void should_create_from_host_name() {
    DefaultEndPoint endPoint = new DefaultEndPoint(new InetSocketAddress("localhost", 9042));
    assertThat(endPoint.asMetricPrefix()).isEqualTo("localhost:9042");
  }

  @Test
  public void should_create_from_literal_ipv4_address() {
    DefaultEndPoint endPoint = new DefaultEndPoint(new InetSocketAddress("127.0.0.1", 9042));
    assertThat(endPoint.asMetricPrefix()).isEqualTo("127_0_0_1:9042");
  }

  @Test
  public void should_create_from_literal_ipv6_address() {
    DefaultEndPoint endPoint = new DefaultEndPoint(new InetSocketAddress("::1", 9042));
    assertThat(endPoint.asMetricPrefix()).isEqualTo("0:0:0:0:0:0:0:1:localhost:9042");
  }

  @Test
  public void should_create_from_ipv4_exact_type_address() throws UnknownHostException {
    // given
    byte[] ipAddr = new byte[] {127, 0, 0, 1};
    InetAddress inet4Address = Inet4Address.getByAddress("localhost", ipAddr);
    // when
    DefaultEndPoint defaultEndPoint =
        new DefaultEndPoint(new InetSocketAddress(inet4Address, 9042));
    // then
    assertThat(defaultEndPoint.asMetricPrefix()).isEqualTo("localhost:9042");
  }

  @Test
  public void should_create_from_ipv6_exact_type_address() throws UnknownHostException {
    // given
    byte[] ipAddr = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    InetAddress inet4Address = Inet6Address.getByAddress("localhost", ipAddr);
    // when
    DefaultEndPoint defaultEndPoint =
        new DefaultEndPoint(new InetSocketAddress(inet4Address, 9042));
    // then
    assertThat(defaultEndPoint.asMetricPrefix())
        .isEqualTo("localhost:102:304:506:708:90a:b0c:d0e:f10:9042");
  }

  @Test
  public void should_create_from_unresolved_address() {
    InetSocketAddress address = InetSocketAddress.createUnresolved("test.com", 9042);
    DefaultEndPoint endPoint = new DefaultEndPoint(address);
    assertThat(endPoint.asMetricPrefix()).isEqualTo("test_com:9042");
    assertThat(address.isUnresolved()).isTrue();
  }

  @Test
  public void should_reject_null_address() {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("address can't be null");

    new DefaultEndPoint(null);
  }
}
