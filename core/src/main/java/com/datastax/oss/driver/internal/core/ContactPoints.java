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
package com.datastax.oss.driver.internal.core;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class to handle the initial contact points passed to the driver. */
public class ContactPoints {
  private static final Logger LOG = LoggerFactory.getLogger(ContactPoints.class);

  public static Set<InetSocketAddress> merge(
      Set<InetSocketAddress> programmaticContactPoints, List<String> configContactPoints) {

    Set<InetSocketAddress> result = Sets.newHashSet(programmaticContactPoints);
    for (String spec : configContactPoints) {
      for (InetSocketAddress address : extract(spec)) {
        boolean wasNew = result.add(address);
        if (!wasNew) {
          LOG.warn("Duplicate contact point {}", address);
        }
      }
    }
    return ImmutableSet.copyOf(result);
  }

  private static Set<InetSocketAddress> extract(String spec) {
    List<String> hostAndPort = Splitter.on(":").splitToList(spec);
    if (hostAndPort.size() != 2) {
      LOG.warn("Ignoring invalid contact point {} (expecting host:port)", spec);
      return Collections.emptySet();
    }
    String host = hostAndPort.get(0);
    int port;
    try {
      port = Integer.parseInt(hostAndPort.get(1));
    } catch (NumberFormatException e) {
      LOG.warn(
          "Ignoring invalid contact point {} (expecting a number, got {})",
          spec,
          hostAndPort.get(1));
      return Collections.emptySet();
    }
    try {
      InetAddress[] inetAddresses = InetAddress.getAllByName(host);
      if (inetAddresses.length > 1) {
        LOG.info(
            "Contact point {} resolves to multiple addresses, will use them all ({})",
            spec,
            Arrays.deepToString(inetAddresses));
      }
      Set<InetSocketAddress> result = new HashSet<>();
      for (InetAddress inetAddress : inetAddresses) {
        result.add(new InetSocketAddress(inetAddress, port));
      }
      return result;
    } catch (UnknownHostException e) {
      LOG.warn("Ignoring invalid contact point {} (unknown host {})", spec, host);
      return Collections.emptySet();
    }
  }
}
