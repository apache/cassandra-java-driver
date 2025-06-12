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

import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.base.Splitter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

class Subnet {
  private final byte[] subnet;
  private final byte[] networkMask;
  private final byte[] upper;
  private final byte[] lower;

  private Subnet(byte[] subnet, byte[] networkMask) {
    this.subnet = subnet;
    this.networkMask = networkMask;

    byte[] upper = new byte[subnet.length];
    byte[] lower = new byte[subnet.length];
    for (int i = 0; i < subnet.length; i++) {
      upper[i] = (byte) (subnet[i] | ~networkMask[i]);
      lower[i] = (byte) (subnet[i] & networkMask[i]);
    }
    this.upper = upper;
    this.lower = lower;
  }

  static Subnet parse(String subnetCIDR) throws UnknownHostException {
    List<String> parts = Splitter.on("/").splitToList(subnetCIDR);
    if (parts.size() != 2) {
      throw new IllegalArgumentException("Invalid subnet: " + subnetCIDR);
    }

    boolean isIPv6 = parts.get(0).contains(":");
    byte[] subnet = InetAddress.getByName(parts.get(0)).getAddress();
    if (isIPv4(subnet) && isIPv6) {
      subnet = toIPv6(subnet);
    }
    int prefixLength = Integer.parseInt(parts.get(1));
    validatePrefixLength(subnet, prefixLength);

    byte[] networkMask = toNetworkMask(subnet, prefixLength);
    validateSubnetIsPrefixBlock(subnet, networkMask, subnetCIDR);
    return new Subnet(subnet, networkMask);
  }

  private static byte[] toNetworkMask(byte[] subnet, int prefixLength) {
    int fullBytes = prefixLength / 8;
    int remainingBits = prefixLength % 8;
    byte[] mask = new byte[subnet.length];
    Arrays.fill(mask, 0, fullBytes, (byte) 0xFF);
    if (remainingBits > 0) {
      mask[fullBytes] = (byte) (0xFF << (8 - remainingBits));
    }
    return mask;
  }

  private static void validatePrefixLength(byte[] subnet, int prefixLength) {
    int max_prefix_length = subnet.length * 8;
    if (prefixLength < 0 || max_prefix_length < prefixLength) {
      throw new IllegalArgumentException(
          String.format(
              "Prefix length %s must be within [0; %s]", prefixLength, max_prefix_length));
    }
  }

  private static void validateSubnetIsPrefixBlock(
      byte[] subnet, byte[] networkMask, String subnetCIDR) {
    byte[] prefixBlock = toPrefixBlock(subnet, networkMask);
    if (!Arrays.equals(subnet, prefixBlock)) {
      throw new IllegalArgumentException(
          String.format("Subnet %s must be represented as a network prefix block", subnetCIDR));
    }
  }

  private static byte[] toPrefixBlock(byte[] subnet, byte[] networkMask) {
    byte[] prefixBlock = new byte[subnet.length];
    for (int i = 0; i < subnet.length; i++) {
      prefixBlock[i] = (byte) (subnet[i] & networkMask[i]);
    }
    return prefixBlock;
  }

  @VisibleForTesting
  byte[] getSubnet() {
    return Arrays.copyOf(subnet, subnet.length);
  }

  @VisibleForTesting
  byte[] getNetworkMask() {
    return Arrays.copyOf(networkMask, networkMask.length);
  }

  byte[] getUpper() {
    return Arrays.copyOf(upper, upper.length);
  }

  byte[] getLower() {
    return Arrays.copyOf(lower, lower.length);
  }

  boolean isIPv4() {
    return isIPv4(subnet);
  }

  boolean isIPv6() {
    return isIPv6(subnet);
  }

  boolean contains(byte[] ip) {
    if (isIPv4() && !isIPv4(ip)) {
      return false;
    }
    if (isIPv6() && isIPv4(ip)) {
      ip = toIPv6(ip);
    }
    if (subnet.length != ip.length) {
      throw new IllegalArgumentException(
          "IP version is unknown: " + Arrays.toString(toZeroBasedByteArray(ip)));
    }
    for (int i = 0; i < subnet.length; i++) {
      if (subnet[i] != (byte) (ip[i] & networkMask[i])) {
        return false;
      }
    }
    return true;
  }

  private static boolean isIPv4(byte[] ip) {
    return ip.length == 4;
  }

  private static boolean isIPv6(byte[] ip) {
    return ip.length == 16;
  }

  private static byte[] toIPv6(byte[] ipv4) {
    byte[] ipv6 = new byte[16];
    ipv6[10] = (byte) 0xFF;
    ipv6[11] = (byte) 0xFF;
    System.arraycopy(ipv4, 0, ipv6, 12, 4);
    return ipv6;
  }

  @Override
  public String toString() {
    return Arrays.toString(toZeroBasedByteArray(subnet));
  }

  private static int[] toZeroBasedByteArray(byte[] bytes) {
    int[] res = new int[bytes.length];
    for (int i = 0; i < bytes.length; i++) {
      res[i] = bytes[i] & 0xFF;
    }
    return res;
  }
}
