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
import static org.mockito.Mockito.mock;

import java.net.InetSocketAddress;
import org.junit.Test;

public class SubnetAddressTest {
  @Test
  public void should_return_return_true_on_overlapping_with_another_subnet_address() {
    SubnetAddress subnetAddress1 =
        new SubnetAddress("100.64.0.0/15", mock(InetSocketAddress.class));
    SubnetAddress subnetAddress2 =
        new SubnetAddress("100.65.0.0/16", mock(InetSocketAddress.class));
    assertThat(subnetAddress1.isOverlapping(subnetAddress2)).isTrue();
  }

  @Test
  public void should_return_return_false_on_not_overlapping_with_another_subnet_address() {
    SubnetAddress subnetAddress1 =
        new SubnetAddress("100.64.0.0/15", mock(InetSocketAddress.class));
    SubnetAddress subnetAddress2 =
        new SubnetAddress("100.66.0.0/15", mock(InetSocketAddress.class));
    assertThat(subnetAddress1.isOverlapping(subnetAddress2)).isFalse();
  }
}
