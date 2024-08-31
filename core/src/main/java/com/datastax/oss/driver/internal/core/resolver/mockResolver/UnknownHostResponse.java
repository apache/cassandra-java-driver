package com.datastax.oss.driver.internal.core.resolver.mockResolver;

import java.net.InetAddress;
import java.net.UnknownHostException;

/** Unsuccessful response. Throws on {@code result()} calls. */
@SuppressWarnings("unused")
public class UnknownHostResponse implements Response {
  private final String message;

  @SuppressWarnings("unused")
  public UnknownHostResponse(String message) {
    this.message = message;
  }

  @Override
  public InetAddress[] result() throws UnknownHostException {
    throw new UnknownHostException(message);
  }
}
