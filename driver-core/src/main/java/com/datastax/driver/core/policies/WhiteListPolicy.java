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
package com.datastax.driver.core.policies;

import com.datastax.driver.core.Host;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;

/**
 * A load balancing policy wrapper that ensure that only hosts from a provided white list will ever
 * be returned.
 *
 * <p>This policy wraps another load balancing policy and will delegate the choice of hosts to the
 * wrapped policy with the exception that only hosts contained in the white list provided when
 * constructing this policy will ever be returned. Any host not in the while list will be considered
 * {@code IGNORED} and thus will not be connected to.
 *
 * <p>This policy can be useful to ensure that the driver only connects to a predefined set of
 * hosts. Keep in mind however that this policy defeats somewhat the host auto-detection of the
 * driver. As such, this policy is only useful in a few special cases or for testing, but is not
 * optimal in general. If all you want to do is limiting connections to hosts of the local
 * data-center then you should use DCAwareRoundRobinPolicy and *not* this policy in particular.
 *
 * @see HostFilterPolicy
 */
public class WhiteListPolicy extends HostFilterPolicy {

  /**
   * Creates a new policy that wraps the provided child policy but only "allows" hosts from the
   * provided white list.
   *
   * @param childPolicy the wrapped policy.
   * @param whiteList the white listed hosts. Only hosts from this list may get connected to
   *     (whether they will get connected to or not depends on the child policy).
   */
  public WhiteListPolicy(LoadBalancingPolicy childPolicy, Collection<InetSocketAddress> whiteList) {
    super(childPolicy, buildPredicate(whiteList));
  }

  /**
   * Private constructor solely for maintaining type from policy created by {@link
   * #ofHosts(LoadBalancingPolicy, String...)}.
   */
  private WhiteListPolicy(LoadBalancingPolicy childPolicy, Predicate<Host> predicate) {
    super(childPolicy, predicate);
  }

  private static Predicate<Host> buildPredicate(Collection<InetSocketAddress> whiteList) {
    final ImmutableSet<InetSocketAddress> hosts = ImmutableSet.copyOf(whiteList);
    return new Predicate<Host>() {
      @Override
      public boolean apply(Host host) {
        // This policy shouldn't be used with endpoints that don't resolve to unique addresses. This
        // should be pretty obvious from the API. We don't really have any way to check it here.
        InetSocketAddress socketAddress = host.getEndPoint().resolve();
        return hosts.contains(socketAddress);
      }
    };
  }

  /**
   * Creates a new policy with the given host names.
   *
   * <p>See {@link #ofHosts(LoadBalancingPolicy, Iterable)} for more details.
   */
  public static WhiteListPolicy ofHosts(LoadBalancingPolicy childPolicy, String... hostnames) {
    return ofHosts(childPolicy, Arrays.asList(hostnames));
  }

  /**
   * Creates a new policy that wraps the provided child policy but only "allows" hosts having
   * addresses that match those from the resolved input host names.
   *
   * <p>Note that all host names must be non-null and resolvable; if <em>any</em> of them cannot be
   * resolved, this method will fail.
   *
   * @param childPolicy the wrapped policy.
   * @param hostnames list of host names to resolve whitelisted addresses from.
   * @throws IllegalArgumentException if any of the given {@code hostnames} could not be resolved.
   * @throws NullPointerException If null was provided for a hostname.
   * @throws SecurityException if a security manager is present and permission to resolve the host
   *     name is denied.
   */
  public static WhiteListPolicy ofHosts(
      LoadBalancingPolicy childPolicy, Iterable<String> hostnames) {
    ImmutableSet.Builder<InetAddress> builder = ImmutableSet.builder();
    for (String hostname : hostnames) {
      try {
        // We explicitly check for nulls because InetAdress.getByName() will happily
        // accept it and use localhost (while a null here almost likely mean a user error,
        // not "connect to localhost")
        if (hostname == null) throw new NullPointerException();
        builder.add(InetAddress.getAllByName(hostname));
      } catch (UnknownHostException e) {
        throw new IllegalArgumentException("Failed to resolve: " + hostname, e);
      }
    }
    final ImmutableSet<InetAddress> addresses = builder.build();
    return new WhiteListPolicy(
        childPolicy,
        new Predicate<Host>() {
          @Override
          public boolean apply(Host host) {
            InetSocketAddress socketAddress = host.getEndPoint().resolve();
            return addresses.contains(socketAddress.getAddress());
          }
        });
  }
}
