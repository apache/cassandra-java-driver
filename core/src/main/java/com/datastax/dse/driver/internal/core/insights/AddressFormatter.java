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
package com.datastax.dse.driver.internal.core.insights;

import java.net.InetAddress;
import java.net.InetSocketAddress;

class AddressFormatter {

  static String nullSafeToString(Object address) {
    if (address instanceof InetAddress) {
      return nullSafeToString((InetAddress) address);
    } else if (address instanceof InetSocketAddress) {
      return nullSafeToString((InetSocketAddress) address);
    } else if (address instanceof String) {
      return address.toString();
    } else {
      return "";
    }
  }

  static String nullSafeToString(InetAddress inetAddress) {
    return inetAddress != null ? inetAddress.getHostAddress() : null;
  }

  static String nullSafeToString(InetSocketAddress inetSocketAddress) {
    if (inetSocketAddress != null) {
      if (inetSocketAddress.isUnresolved()) {
        return String.format(
            "%s:%s",
            nullSafeToString(inetSocketAddress.getHostName()), inetSocketAddress.getPort());
      } else {
        return String.format(
            "%s:%s", nullSafeToString(inetSocketAddress.getAddress()), inetSocketAddress.getPort());
      }
    }
    return null;
  }
}
