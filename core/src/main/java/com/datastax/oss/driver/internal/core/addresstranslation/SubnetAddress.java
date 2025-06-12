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

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

class SubnetAddress {
  private final Subnet subnet;
  private final InetSocketAddress address;

  SubnetAddress(String subnetCIDR, InetSocketAddress address) {
    try {
      this.subnet = Subnet.parse(subnetCIDR);
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
    this.address = address;
  }

  InetSocketAddress getAddress() {
    return this.address;
  }

  boolean isOverlapping(SubnetAddress other) {
    Subnet thisSubnet = this.subnet;
    Subnet otherSubnet = other.subnet;
    return thisSubnet.contains(otherSubnet.getLower())
        || thisSubnet.contains(otherSubnet.getUpper())
        || otherSubnet.contains(thisSubnet.getLower())
        || otherSubnet.contains(thisSubnet.getUpper());
  }

  boolean contains(InetSocketAddress address) {
    return subnet.contains(address.getAddress().getAddress());
  }

  boolean isIPv4() {
    return subnet.isIPv4();
  }

  boolean isIPv6() {
    return subnet.isIPv6();
  }

  @Override
  public String toString() {
    return "SubnetAddress[subnet=" + subnet + ", address=" + address + "]";
  }
}
