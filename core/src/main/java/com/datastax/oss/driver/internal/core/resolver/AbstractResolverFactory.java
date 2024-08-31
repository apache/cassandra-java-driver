package com.datastax.oss.driver.internal.core.resolver;

public interface AbstractResolverFactory {
  public Resolver getResolver(Class<?> clazz);
}
