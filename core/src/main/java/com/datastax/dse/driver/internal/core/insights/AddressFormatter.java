/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
