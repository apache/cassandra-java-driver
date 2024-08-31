package com.datastax.oss.driver.internal.core.resolver.mockResolver;

import com.datastax.oss.driver.internal.core.resolver.Resolver;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;

/** Resolver implementation that allows returning custom results. */
public class MockResolver implements Resolver {
  private final MockResolverResultSource resultSource;

  MockResolver(MockResolverResultSource resultSource) {
    this.resultSource = resultSource;
  }

  /**
   * Returns first {@link InetAddress} from overrided results {@link Response} if one is present.
   * Otherwise forwards the call to {@link InetAddress#getByName(String)}
   *
   * @param host host to resolve.
   * @return address.
   * @throws UnknownHostException
   */
  @Override
  public InetAddress getByName(String host) throws UnknownHostException {
    Response response = this.resultSource.getResponse(host);
    if (response == null) {
      return InetAddress.getByName(host);
    }
    return response.result()[0];
  }

  /**
   * Returns overrided results for provided {@code host}, if one is present, otherwise the call to
   * Otherwise forwards the call to {@link InetAddress#getAllByName(String)}
   *
   * @param host host to resolve.
   * @return address.
   * @throws UnknownHostException
   */
  @Override
  public InetAddress[] getAllByName(String host) throws UnknownHostException {
    Response response = this.resultSource.getResponse(host);
    if (response == null) {
      return InetAddress.getAllByName(host);
    }
    return response.result();
  }

  /**
   * If {@code addr} is an resolved {@link InetSocketAddress}, or there is no response overriding
   * for the it's host, or {@code addr} is something else, it returns {@code addr} as is. Otherwise
   * it returns overrided response, if overrided response throws {@link UnknownHostException}, it
   * emulates it by returning unresolved {@link InetSocketAddress} with bogus host.
   *
   * @param addr SocketAddress to modify.
   * @return possibly new SocketAddress.
   */
  @Override
  public SocketAddress resolve(SocketAddress addr) {
    if (!(addr instanceof InetSocketAddress)) {
      return addr;
    }
    InetSocketAddress inetSockAddr = ((InetSocketAddress) addr);
    if (!inetSockAddr.isUnresolved()) {
      return addr;
    }
    String hostname = inetSockAddr.getHostName();
    int port = inetSockAddr.getPort();
    Response response = this.resultSource.getResponse(hostname);
    if (response == null) {
      return addr;
    }
    try {
      return new InetSocketAddress(response.result()[0], port);
    } catch (UnknownHostException e) {
      return InetSocketAddress.createUnresolved(hostname + ".bad.host.115t87", port);
    }
  }
}
