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
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.ADDRESS_TRANSLATOR_RESOLVE_ADDRESSES;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.ADDRESS_TRANSLATOR_SUBNET_ADDRESSES;

import com.datastax.oss.driver.api.core.addresstranslation.AddressTranslator;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.internal.core.util.AddressUtils;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This translator returns the proxy address of the private subnet containing the Cassandra node IP,
 * or default address if no matching subnets, or passes through the original node address if no
 * default configured.
 *
 * <p>The translator can be used for scenarios when all nodes are behind some kind of proxy, and
 * that proxy is different for nodes located in different subnets (eg. when Cassandra is deployed in
 * multiple datacenters/regions). One can use this, for example, for Cassandra on Kubernetes with
 * different Cassandra datacenters deployed to different Kubernetes clusters.
 */
public class SubnetAddressTranslator implements AddressTranslator {
  private static final Logger LOG = LoggerFactory.getLogger(SubnetAddressTranslator.class);

  private final List<SubnetAddress> subnetAddresses;

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private final Optional<InetSocketAddress> defaultAddress;

  private final String logPrefix;

  public SubnetAddressTranslator(@NonNull DriverContext context) {
    logPrefix = context.getSessionName();
    boolean resolveAddresses =
        context
            .getConfig()
            .getDefaultProfile()
            .getBoolean(ADDRESS_TRANSLATOR_RESOLVE_ADDRESSES, false);
    this.subnetAddresses =
        context.getConfig().getDefaultProfile().getStringMap(ADDRESS_TRANSLATOR_SUBNET_ADDRESSES)
            .entrySet().stream()
            .map(
                e -> {
                  // Quoted and/or containing forward slashes map keys in reference.conf are read to
                  // strings with additional quotes, eg. 100.64.0.0/15 -> '100.64.0."0/15"' or
                  // "100.64.0.0/15" -> '"100.64.0.0/15"'
                  String subnetCIDR = e.getKey().replaceAll("\"", "");
                  String address = e.getValue();
                  return new SubnetAddress(subnetCIDR, parseAddress(address, resolveAddresses));
                })
            .collect(Collectors.toList());
    this.defaultAddress =
        Optional.ofNullable(
                context
                    .getConfig()
                    .getDefaultProfile()
                    .getString(ADDRESS_TRANSLATOR_DEFAULT_ADDRESS, null))
            .map(address -> parseAddress(address, resolveAddresses));

    validateSubnetsAreOfSameProtocol(this.subnetAddresses);
    validateSubnetsAreNotOverlapping(this.subnetAddresses);
  }

  private static void validateSubnetsAreOfSameProtocol(List<SubnetAddress> subnets) {
    for (int i = 0; i < subnets.size() - 1; i++) {
      for (int j = i + 1; j < subnets.size(); j++) {
        SubnetAddress subnet1 = subnets.get(i);
        SubnetAddress subnet2 = subnets.get(j);
        if (subnet1.isIPv4() != subnet2.isIPv4() && subnet1.isIPv6() != subnet2.isIPv6()) {
          throw new IllegalArgumentException(
              String.format(
                  "Configured subnets are of the different protocols: %s, %s", subnet1, subnet2));
        }
      }
    }
  }

  private static void validateSubnetsAreNotOverlapping(List<SubnetAddress> subnets) {
    for (int i = 0; i < subnets.size() - 1; i++) {
      for (int j = i + 1; j < subnets.size(); j++) {
        SubnetAddress subnet1 = subnets.get(i);
        SubnetAddress subnet2 = subnets.get(j);
        if (subnet1.isOverlapping(subnet2)) {
          throw new IllegalArgumentException(
              String.format("Configured subnets are overlapping: %s, %s", subnet1, subnet2));
        }
      }
    }
  }

  @NonNull
  @Override
  public InetSocketAddress translate(@NonNull InetSocketAddress address) {
    InetSocketAddress translatedAddress = null;
    for (SubnetAddress subnetAddress : subnetAddresses) {
      if (subnetAddress.contains(address)) {
        translatedAddress = subnetAddress.getAddress();
      }
    }
    if (translatedAddress == null && defaultAddress.isPresent()) {
      translatedAddress = defaultAddress.get();
    }
    if (translatedAddress == null) {
      translatedAddress = address;
    }
    LOG.debug("[{}] Translated {} to {}", logPrefix, address, translatedAddress);
    return translatedAddress;
  }

  @Override
  public void close() {}

  @Nullable
  private InetSocketAddress parseAddress(String address, boolean resolve) {
    try {
      InetSocketAddress parsedAddress = AddressUtils.extract(address, resolve).iterator().next();
      LOG.debug("[{}] Parsed {} to {}", logPrefix, address, parsedAddress);
      return parsedAddress;
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(
          String.format("Invalid address %s (%s)", address, e.getMessage()), e);
    }
  }
}
