package com.datastax.oss.driver.internal.core.resolver;

import com.datastax.oss.driver.internal.core.resolver.defaultResolver.DefaultResolverFactory;

/**
 * Entry point for driver components to getting the {@link Resolver} instances. By default returns
 * instances of {@link DefaultResolverFactory}.
 */
public class ResolverProvider {

  private static AbstractResolverFactory defaultResolverFactoryImpl = new DefaultResolverFactory();

  /**
   * Asks factory for new {@link Resolver}.
   *
   * @param clazz Class that is requesting the {@link Resolver}.
   * @return new {@link Resolver}.
   */
  public static Resolver getResolver(Class<?> clazz) {
    return defaultResolverFactoryImpl.getResolver(clazz);
  }

  /**
   * Replaces resolver factory with another, possibly producing different implementation of {@link
   * Resolver}.
   *
   * @param resolverFactoryImpl new {@link Resolver} factory.
   */
  public static void setDefaultResolverFactory(AbstractResolverFactory resolverFactoryImpl) {
    defaultResolverFactoryImpl = resolverFactoryImpl;
  }
}
