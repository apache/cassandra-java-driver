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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.InetSocketAddress;
import org.junit.Test;

public class DefaultEndPointTest {

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
    assertThat(endPoint.asMetricPrefix()).isEqualTo("0:0:0:0:0:0:0:1:9042");
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
    assertThatThrownBy(() -> new DefaultEndPoint(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("address can't be null");
  }
}
