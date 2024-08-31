package com.datastax.oss.driver.internal.core.resolver;

import com.datastax.oss.driver.internal.core.resolver.defaultResolver.DefaultResolver;
import java.net.InetAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;

/**
 * Resolver is a "middleman" class that is meant to serve as a hook point for testing.
 * Any time core driver tries to resolve a host defined by hostname, such attempt will
 * go through the Resolver first. At that moment Resolver may return custom result it
 * was configured to return or simply forward the call to the corresponding InetAddress class
 * method.
 *
 * Some classes are exempt from running through the Resolver and they still call InetAddress directly
 * (e.g. {@link com.datastax.dse.driver.internal.core.insights.InsightsClient}).
 * By default, driver should use {@link DefaultResolver}
 * which should introduce no change in behaviour and redirect all calls to {@link InetAddress).
 */
public interface Resolver {

  /**
   * Replaces calls to {@link InetAddress#getByName(String)}.
   *
   * @param host host to resolve.
   * @return an IP address for the given host name.
   * @throws UnknownHostException
   */
  InetAddress getByName(String host) throws UnknownHostException;

  /**
   * Replaces calls to {@link InetAddress#getAllByName(String)}
   *
   * @param host host to resolve.
   * @return an array of all the IP addresses for a given host name.
   * @throws UnknownHostException
   */
  InetAddress[] getAllByName(String host) throws UnknownHostException;

  /**
   * Resolves {@link SocketAddress} returning {@link SocketAddress} To be called just before passing
   * {@link SocketAddress}, that may be unresolved, to an another library that is going resolve it.
   *
   * @param addr SocketAddress with host details
   * @return SocketAddress instance with possibly modified contents
   */
  SocketAddress resolve(SocketAddress addr);
}
