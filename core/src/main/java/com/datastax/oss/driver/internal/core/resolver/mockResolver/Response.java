package com.datastax.oss.driver.internal.core.resolver.mockResolver;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Represents results returned by {@link
 * com.datastax.oss.driver.internal.core.resolver.mockResolver.MockResolverFactory}.
 */
public interface Response {
  /**
   * Underlying result of this Response.
   *
   * @return result of this Response.
   * @throws UnknownHostException
   */
  InetAddress[] result() throws UnknownHostException;
}
