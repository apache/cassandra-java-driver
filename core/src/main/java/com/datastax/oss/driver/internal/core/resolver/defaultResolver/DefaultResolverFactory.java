package com.datastax.oss.driver.internal.core.resolver.defaultResolver;

import com.datastax.oss.driver.internal.core.resolver.AbstractResolverFactory;

/**
 * Produces instances of {@link
 * com.datastax.oss.driver.internal.core.resolver.defaultResolver.DefaultResolver}.
 */
public class DefaultResolverFactory implements AbstractResolverFactory {

  /**
   * Returns new {@link
   * com.datastax.oss.driver.internal.core.resolver.defaultResolver.DefaultResolver} instance.
   *
   * @param clazz Class for which the instance is being created.
   * @return new {@link
   *     com.datastax.oss.driver.internal.core.resolver.defaultResolver.DefaultResolver} instance.
   */
  @Override
  public DefaultResolver getResolver(Class<?> clazz) {
    return new DefaultResolver();
  }
}
