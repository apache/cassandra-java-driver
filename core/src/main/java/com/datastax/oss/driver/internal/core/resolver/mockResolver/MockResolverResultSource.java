package com.datastax.oss.driver.internal.core.resolver.mockResolver;

public interface MockResolverResultSource {
  Response getResponse(String hostname);
}
