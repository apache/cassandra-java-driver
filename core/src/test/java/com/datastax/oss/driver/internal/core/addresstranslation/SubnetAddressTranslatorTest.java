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

import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.ADDRESS_TRANSLATOR_DEFAULT_ADDRESS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.ADDRESS_TRANSLATOR_SUBNET_ADDRESSES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.context.MockedDriverContextFactory;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;

@SuppressWarnings("resource")
public class SubnetAddressTranslatorTest {

  @Test
  public void should_translate_to_correct_subnet_address_ipv4() {
    Map<String, String> subnetAddresses =
        ImmutableMap.of(
            "\"100.64.0.0/15\"", "cassandra.datacenter1.com:19042",
            "100.66.0.\"0/15\"", "cassandra.datacenter2.com:19042");
    DefaultDriverContext context = context(subnetAddresses);
    SubnetAddressTranslator translator = new SubnetAddressTranslator(context);
    InetSocketAddress address = new InetSocketAddress("100.64.0.1", 9042);
    assertThat(translator.translate(address))
        .isEqualTo(InetSocketAddress.createUnresolved("cassandra.datacenter1.com", 19042));
  }

  @Test
  public void should_translate_to_correct_subnet_address_ipv6() {
    Map<String, String> subnetAddresses =
        ImmutableMap.of(
            "\"::ffff:6440:0/111\"", "cassandra.datacenter1.com:19042",
            "\"::ffff:6442:0/111\"", "cassandra.datacenter2.com:19042");
    DefaultDriverContext context = context(subnetAddresses);
    SubnetAddressTranslator translator = new SubnetAddressTranslator(context);
    InetSocketAddress address = new InetSocketAddress("::ffff:6440:1", 9042);
    assertThat(translator.translate(address))
        .isEqualTo(InetSocketAddress.createUnresolved("cassandra.datacenter1.com", 19042));
  }

  @Test
  public void should_translate_to_default_address() {
    DefaultDriverContext context = context(ImmutableMap.of());
    when(context
            .getConfig()
            .getDefaultProfile()
            .getString(ADDRESS_TRANSLATOR_DEFAULT_ADDRESS, null))
        .thenReturn("cassandra.com:19042");
    SubnetAddressTranslator translator = new SubnetAddressTranslator(context);
    InetSocketAddress address = new InetSocketAddress("100.68.0.1", 9042);
    assertThat(translator.translate(address))
        .isEqualTo(InetSocketAddress.createUnresolved("cassandra.com", 19042));
  }

  @Test
  public void should_pass_through_not_matched_address() {
    DefaultDriverContext context = context(ImmutableMap.of());
    SubnetAddressTranslator translator = new SubnetAddressTranslator(context);
    InetSocketAddress address = new InetSocketAddress("100.68.0.1", 9042);
    assertThat(translator.translate(address)).isEqualTo(address);
  }

  @Test
  public void should_fail_on_intersecting_subnets_ipv4() {
    Map<String, String> subnetAddresses =
        ImmutableMap.of(
            "\"100.64.0.0/15\"", "cassandra.datacenter1.com:19042",
            "100.65.0.\"0/16\"", "cassandra.datacenter2.com:19042");
    DefaultDriverContext context = context(subnetAddresses);
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new SubnetAddressTranslator(context))
        .withMessage(
            "Configured subnets are overlapping: "
                + String.format(
                    "SubnetAddress[subnet=[100, 64, 0, 0], address=%s], ",
                    InetSocketAddress.createUnresolved("cassandra.datacenter1.com", 19042))
                + String.format(
                    "SubnetAddress[subnet=[100, 65, 0, 0], address=%s]",
                    InetSocketAddress.createUnresolved("cassandra.datacenter2.com", 19042)));
  }

  @Test
  public void should_fail_on_intersecting_subnets_ipv6() {
    Map<String, String> subnetAddresses =
        ImmutableMap.of(
            "\"::ffff:6440:0/111\"", "cassandra.datacenter1.com:19042",
            "\"::ffff:6441:0/112\"", "cassandra.datacenter2.com:19042");
    DefaultDriverContext context = context(subnetAddresses);
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new SubnetAddressTranslator(context))
        .withMessage(
            "Configured subnets are overlapping: "
                + String.format(
                    "SubnetAddress[subnet=[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 100, 64, 0, 0], address=%s], ",
                    InetSocketAddress.createUnresolved("cassandra.datacenter1.com", 19042))
                + String.format(
                    "SubnetAddress[subnet=[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 100, 65, 0, 0], address=%s]",
                    InetSocketAddress.createUnresolved("cassandra.datacenter2.com", 19042)));
  }

  @Test
  public void should_fail_on_subnet_address_without_port() {
    Map<String, String> subnetAddresses =
        ImmutableMap.of("\"100.64.0.0/15\"", "cassandra.datacenter1.com");
    DefaultDriverContext context = context(subnetAddresses);
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new SubnetAddressTranslator(context))
        .withMessage("Invalid address cassandra.datacenter1.com (expecting format host:port)");
  }

  @Test
  public void should_fail_on_default_address_without_port() {
    DefaultDriverContext context = context(ImmutableMap.of());
    when(context
            .getConfig()
            .getDefaultProfile()
            .getString(ADDRESS_TRANSLATOR_DEFAULT_ADDRESS, null))
        .thenReturn("cassandra.com");
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new SubnetAddressTranslator(context))
        .withMessage("Invalid address cassandra.com (expecting format host:port)");
  }

  private static DefaultDriverContext context(Map<String, String> subnetAddresses) {
    DriverExecutionProfile profile = mock(DriverExecutionProfile.class);
    when(profile.getStringMap(ADDRESS_TRANSLATOR_SUBNET_ADDRESSES)).thenReturn(subnetAddresses);
    return MockedDriverContextFactory.defaultDriverContext(Optional.of(profile));
  }
}
