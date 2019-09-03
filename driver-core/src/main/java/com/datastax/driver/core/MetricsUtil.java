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
package com.datastax.driver.core;

import java.net.InetAddress;

public class MetricsUtil {

  public static String hostMetricName(String prefix, Host host) {
    EndPoint endPoint = host.getEndPoint();
    if (endPoint instanceof TranslatedAddressEndPoint) {
      InetAddress address = endPoint.resolve().getAddress();
      return hostMetricNameFromAddress(prefix, address);
    } else {
      // We have no guarantee that endpoints resolve to unique addresses
      return prefix + endPoint.toString();
    }
  }

  private static String hostMetricNameFromAddress(String prefix, InetAddress address) {
    StringBuilder result = new StringBuilder(prefix);
    boolean first = true;
    for (byte b : address.getAddress()) {
      if (first) {
        first = false;
      } else {
        result.append('_');
      }
      result.append(b & 0xFF);
    }
    return result.toString();
  }
}
