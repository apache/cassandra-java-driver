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
package com.datastax.oss.driver.internal.core.util;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

public class AddressUtils {

  public static Set<InetSocketAddress> extract(String address, boolean resolve) {
    int separator = address.lastIndexOf(':');
    if (separator < 0) {
      throw new IllegalArgumentException("expecting format host:port");
    }

    String host = address.substring(0, separator);
    String portString = address.substring(separator + 1);
    int port;
    try {
      port = Integer.parseInt(portString);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("expecting port to be a number, got " + portString, e);
    }
    if (!resolve) {
      return ImmutableSet.of(InetSocketAddress.createUnresolved(host, port));
    } else {
      InetAddress[] inetAddresses;
      try {
        inetAddresses = InetAddress.getAllByName(host);
      } catch (UnknownHostException e) {
        throw new RuntimeException(e);
      }
      Set<InetSocketAddress> result = new HashSet<>();
      for (InetAddress inetAddress : inetAddresses) {
        result.add(new InetSocketAddress(inetAddress, port));
      }
      return result;
    }
  }
}
