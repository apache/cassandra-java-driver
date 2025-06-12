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
package com.datastax.oss.driver.internal.core.addresstranslation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNoException;

import java.net.UnknownHostException;
import org.junit.Test;

public class SubnetTest {
  @Test
  public void should_parse_to_correct_ipv4_subnet() throws UnknownHostException {
    Subnet subnet = Subnet.parse("100.64.0.0/15");
    assertThat(subnet.getSubnet()).containsExactly(100, 64, 0, 0);
    assertThat(subnet.getNetworkMask()).containsExactly(255, 254, 0, 0);
    assertThat(subnet.getUpper()).containsExactly(100, 65, 255, 255);
    assertThat(subnet.getLower()).containsExactly(100, 64, 0, 0);
  }

  @Test
  public void should_parse_to_correct_ipv6_subnet() throws UnknownHostException {
    Subnet subnet = Subnet.parse("2001:db8:85a3::8a2e:370:0/111");
    assertThat(subnet.getSubnet())
        .containsExactly(32, 1, 13, 184, 133, 163, 0, 0, 0, 0, 138, 46, 3, 112, 0, 0);
    assertThat(subnet.getNetworkMask())
        .containsExactly(
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 254, 0, 0);
    assertThat(subnet.getUpper())
        .containsExactly(32, 1, 13, 184, 133, 163, 0, 0, 0, 0, 138, 46, 3, 113, 255, 255);
    assertThat(subnet.getLower())
        .containsExactly(32, 1, 13, 184, 133, 163, 0, 0, 0, 0, 138, 46, 3, 112, 0, 0);
  }

  @Test
  public void should_parse_to_correct_ipv6_subnet_ipv4_convertible() throws UnknownHostException {
    Subnet subnet = Subnet.parse("::ffff:6440:0/111");
    assertThat(subnet.getSubnet())
        .containsExactly(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 100, 64, 0, 0);
    assertThat(subnet.getNetworkMask())
        .containsExactly(
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 254, 0, 0);
    assertThat(subnet.getUpper())
        .containsExactly(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 100, 65, 255, 255);
    assertThat(subnet.getLower())
        .containsExactly(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 100, 64, 0, 0);
  }

  @Test
  public void should_fail_on_invalid_cidr_format() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> Subnet.parse("invalid"))
        .withMessage("Invalid subnet: invalid");
  }

  @Test
  public void should_parse_bounding_prefix_lengths_correctly() {
    assertThatNoException().isThrownBy(() -> Subnet.parse("0.0.0.0/0"));
    assertThatNoException().isThrownBy(() -> Subnet.parse("100.64.0.0/32"));
  }

  @Test
  public void should_fail_on_invalid_prefix_length() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> Subnet.parse("100.64.0.0/-1"))
        .withMessage("Prefix length -1 must be within [0; 32]");
    assertThatIllegalArgumentException()
        .isThrownBy(() -> Subnet.parse("100.64.0.0/33"))
        .withMessage("Prefix length 33 must be within [0; 32]");
  }

  @Test
  public void should_fail_on_not_prefix_block_subnet_ipv4() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> Subnet.parse("100.65.0.0/15"))
        .withMessage("Subnet 100.65.0.0/15 must be represented as a network prefix block");
  }

  @Test
  public void should_fail_on_not_prefix_block_subnet_ipv6() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> Subnet.parse("::ffff:6441:0/111"))
        .withMessage("Subnet ::ffff:6441:0/111 must be represented as a network prefix block");
  }

  @Test
  public void should_return_true_on_containing_address() throws UnknownHostException {
    Subnet subnet = Subnet.parse("100.64.0.0/15");
    assertThat(subnet.contains(new byte[] {100, 64, 0, 0})).isTrue();
    assertThat(subnet.contains(new byte[] {100, 65, (byte) 255, (byte) 255})).isTrue();
    assertThat(subnet.contains(new byte[] {100, 65, 100, 100})).isTrue();
  }

  @Test
  public void should_return_false_on_not_containing_address() throws UnknownHostException {
    Subnet subnet = Subnet.parse("100.64.0.0/15");
    assertThat(subnet.contains(new byte[] {100, 63, (byte) 255, (byte) 255})).isFalse();
    assertThat(subnet.contains(new byte[] {100, 66, 0, 0})).isFalse();
    // IPv6 cannot be contained by IPv4 subnet.
    assertThat(subnet.contains(new byte[16])).isFalse();
  }
}
