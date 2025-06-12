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
package com.datastax.oss.driver.internal.core;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.driver.internal.core.util.AddressUtils;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class to handle the initial contact points passed to the driver. */
public class ContactPoints {
  private static final Logger LOG = LoggerFactory.getLogger(ContactPoints.class);

  public static Set<EndPoint> merge(
      Set<EndPoint> programmaticContactPoints, List<String> configContactPoints, boolean resolve) {

    Set<EndPoint> result = Sets.newHashSet(programmaticContactPoints);
    for (String spec : configContactPoints) {

      Set<InetSocketAddress> addresses = Collections.emptySet();
      try {
        addresses = AddressUtils.extract(spec, resolve);
      } catch (RuntimeException e) {
        LOG.warn("Ignoring invalid contact point {} ({})", spec, e.getMessage(), e);
      }

      if (addresses.size() > 1) {
        LOG.info(
            "Contact point {} resolves to multiple addresses, will use them all ({})",
            spec,
            addresses);
      }

      for (InetSocketAddress address : addresses) {
        DefaultEndPoint endPoint = new DefaultEndPoint(address);
        boolean wasNew = result.add(endPoint);
        if (!wasNew) {
          LOG.warn("Duplicate contact point {}", address);
        }
      }
    }
    return ImmutableSet.copyOf(result);
  }
}
