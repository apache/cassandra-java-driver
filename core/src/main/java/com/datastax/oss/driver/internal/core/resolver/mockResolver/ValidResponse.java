package com.datastax.oss.driver.internal.core.resolver.mockResolver;

import java.net.InetAddress;
import java.net.UnknownHostException;

/** Successful response with an array of valid IPs. */
public class ValidResponse implements Response {
  private final InetAddress[] list;

  public ValidResponse(InetAddress[] list) {
    this.list = list;
  }

  @Override
  public InetAddress[] result() throws UnknownHostException {
    return list;
  }
}
