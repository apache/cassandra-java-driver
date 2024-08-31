package com.datastax.oss.driver.internal.core.resolver.mockResolver;

import com.datastax.oss.driver.internal.core.resolver.AbstractResolverFactory;
import com.datastax.oss.driver.internal.core.resolver.Resolver;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Produces instances of {@link MockResolver}. All {@link Resolver} instances produced by instance
 * of this class return results sourced by the factory. if {@code defaultResponse} is not set, it
 * produces same results as `DefaultResolver` for hosts that are not in the {@code responses}
 */
public class MockResolverFactory implements AbstractResolverFactory, MockResolverResultSource {
  private final ConcurrentMap<String, Response> responses = new ConcurrentHashMap<>();
  private Response defaultResponse;

  @Override
  public Response getResponse(String hostname) {
    return responses.getOrDefault(hostname, defaultResponse);
  }

  /**
   * Set the result for calls to {@link MockResolverFactory#getResponse(String)} for hostnames that
   * do not have custom mappings.
   *
   * @param response new default response.
   */
  @SuppressWarnings("unused")
  public void setDefaultResponse(Response response) {
    this.defaultResponse = response;
  }

  /**
   * Add provided mappings to the current mappings.
   *
   * @param map mappings to merge in.
   */
  @SuppressWarnings("unused")
  public void updateResponses(Map<String, Response> map) {
    responses.putAll(map);
  }

  /**
   * Adds a single response to the current mappings.
   *
   * @param address host - key of the mapping.
   * @param response value of the mapping.
   */
  public void updateResponse(String address, Response response) {
    responses.put(address, response);
  }

  /**
   * Returns new {@link MockResolver} instance.
   *
   * @param clazz Class for which the instance is being created.
   * @return new {@link MockResolver} instance.
   */
  @Override
  public MockResolver getResolver(Class<?> clazz) {
    return new MockResolver(this);
  }
}
