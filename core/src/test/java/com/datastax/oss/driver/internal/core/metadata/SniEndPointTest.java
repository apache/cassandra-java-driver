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
package com.datastax.oss.driver.internal.core.metadata;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.stream.Stream;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SniEndPointTest {
  private static InetSocketAddress SNI_ADDRESS =
      InetSocketAddress.createUnresolved("unittest.host", 12345);
  private static String SERVER_NAME = "unittest.server.name";

  @Spy private SniEndPoint sniEndPoint = new SniEndPoint(SNI_ADDRESS, SERVER_NAME);

  private static InetAddress[] createAddresses(String... addrs) {
    return Stream.of(addrs)
        .map(
            addr -> {
              try {
                int[] comp = Arrays.stream(addr.split("\\.")).mapToInt(Integer::parseInt).toArray();
                return InetAddress.getByAddress(
                    new byte[] {(byte) comp[0], (byte) comp[1], (byte) comp[2], (byte) comp[3]});
              } catch (UnknownHostException e) {
                throw new RuntimeException(e);
              }
            })
        .toArray(InetAddress[]::new);
  }

  private static InetSocketAddress buildResolved(InetAddress addr) {
    return new InetSocketAddress(addr, SNI_ADDRESS.getPort());
  }

  @Test
  public void should_retrieve_unresolved() {
    assertThat(sniEndPoint.retrieve()).isEqualTo(SNI_ADDRESS);
  }

  @Test
  public void should_resolve_resolved() throws UnknownHostException {
    InetAddress[] addrs = createAddresses("10.0.0.1");
    doReturn(addrs).when(sniEndPoint).resolveARecords();

    assertThat(sniEndPoint.resolve()).isNotEqualTo(SNI_ADDRESS).isEqualTo(buildResolved(addrs[0]));
  }

  @Test
  public void should_resolve_roundrobin() throws UnknownHostException {
    InetAddress[] addrs = createAddresses("10.0.0.1", "10.0.0.2", "10.0.0.3");
    doReturn(addrs).when(sniEndPoint).resolveARecords();

    // figure out first returned item
    InetSocketAddress resolved = sniEndPoint.resolve();
    int initial = ArrayUtils.indexOf(addrs, resolved.getAddress());
    assertThat(initial).isNotEqualTo(-1);

    // check that each resolve() call returns the next item in the list
    for (int i = 0; i < 10; i++) {
      assertThat(sniEndPoint.resolve())
          .isNotEqualTo(SNI_ADDRESS)
          .isEqualTo(buildResolved(addrs[(initial + (i + 1)) % addrs.length]));
    }
  }

  @Test
  public void should_handle_offset_wrap() throws UnknownHostException {
    SniEndPoint.OFFSET.set(Integer.MAX_VALUE - 1);

    InetAddress[] addrs = createAddresses("10.0.0.1", "10.0.0.2", "10.0.0.3");
    doReturn(addrs).when(sniEndPoint).resolveARecords();

    // check resolve doesn't fail when we loop back round
    for (int i = 0; i < 10; i++) {
      assertThat(sniEndPoint.resolve()).isNotEqualTo(SNI_ADDRESS);
      if (i == 0) {
        // getAndIncrement returned Integer.MAX_VALUE - 1
        assertThat(SniEndPoint.OFFSET.get()).isEqualTo(Integer.MAX_VALUE);
      } else if (i == 1) {
        // getAndIncrement returned Integer.MAX_VALUE
        assertThat(SniEndPoint.OFFSET.get()).isEqualTo(Integer.MIN_VALUE);
      } else {
        // i == 2: getAndIncrement returned Integer.MIN_VALUE which is
        //         replaced with 0 and OFFSET is set to 1
        // i > 2: getAndIncrement returned i - 2 (OFFSET is one greater)
        assertThat(SniEndPoint.OFFSET.get()).isEqualTo(i - 1);
      }
    }
  }

  @Test
  public void should_fail_if_unable_to_resolve() throws UnknownHostException {
    doThrow(new UnknownHostException("unittest.resolve.failed"))
        .when(sniEndPoint)
        .resolveARecords();

    // resolve throws unable to resolve error
    assertThatCode(() -> sniEndPoint.resolve()).isInstanceOf(IllegalArgumentException.class);

    // retrieve still works
    assertThat(sniEndPoint.retrieve()).isEqualTo(SNI_ADDRESS);
  }

  @Test
  public void should_fail_if_no_resolve_results() throws UnknownHostException {
    doReturn(new InetAddress[0]).when(sniEndPoint).resolveARecords();

    // resolve throws unable to resolve error
    assertThatCode(() -> sniEndPoint.resolve()).isInstanceOf(IllegalArgumentException.class);

    // retrieve still works
    assertThat(sniEndPoint.retrieve()).isEqualTo(SNI_ADDRESS);
  }
}
