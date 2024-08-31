package com.datastax.oss.driver.internal.core.resolver.defaultResolver;

import com.datastax.oss.driver.internal.core.resolver.Resolver;
import java.net.InetAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;

/**
 * Resolver implementation that forwards all calls to {@link InetAddress} or returns the arguments
 * unmodified.
 */
public class DefaultResolver implements Resolver {

  DefaultResolver() {}

  /**
   * Equivalent to {@link InetAddress#getByName(String)}.
   *
   * @throws UnknownHostException
   */
  @Override
  public InetAddress getByName(String host) throws UnknownHostException {
    return InetAddress.getByName(host);
  }

  /**
   * Equivalent to {@link InetAddress#getAllByName(String)}.
   *
   * @throws UnknownHostException
   */
  @Override
  public InetAddress[] getAllByName(String host) throws UnknownHostException {
    return InetAddress.getAllByName(host);
  }

  /**
   * Returns {@link SocketAddress} as is. No reason to resolve it here, it is supposed to be done by
   * a library that is going to consume the result.
   *
   * @param addr {@link SocketAddress}.
   * @return the very same addr.
   */
  @Override
  public SocketAddress resolve(SocketAddress addr) {
    return addr;
  }
}
