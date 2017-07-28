/*
 * Copyright (C) 2017-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.type.codec.registry;

import com.datastax.oss.driver.api.core.DriverExecutionException;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default codec registry implementation.
 *
 * <p>It is a caching registry based on Guava cache (note that the driver shades Guava).
 */
public class DefaultCodecRegistry extends CachingCodecRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultCodecRegistry.class);

  private final LoadingCache<CacheKey, TypeCodec<?>> cache;

  /**
   * Creates a new instance, with some amount of control over the cache behavior.
   *
   * <p>Giving full access to the Guava cache API would be too much work, since it is shaded and we
   * have to wrap everything. If you need something that's not available here, it's easy enough to
   * write your own CachingCodecRegistry implementation. It's doubtful that stuff like cache
   * eviction is that useful anyway.
   */
  public DefaultCodecRegistry(
      String logPrefix,
      int initialCacheCapacity,
      BiFunction<CacheKey, TypeCodec<?>, Integer> cacheWeigher,
      int maximumCacheWeight,
      BiConsumer<CacheKey, TypeCodec<?>> cacheRemovalListener,
      TypeCodec<?>... userCodecs) {

    super(logPrefix, userCodecs);
    CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
    if (initialCacheCapacity > 0) {
      cacheBuilder.initialCapacity(initialCacheCapacity);
    }
    if (cacheWeigher != null) {
      cacheBuilder.weigher(cacheWeigher::apply).maximumWeight(maximumCacheWeight);
    }
    if (cacheRemovalListener != null) {
      //noinspection ResultOfMethodCallIgnored
      cacheBuilder.removalListener(
          (RemovalListener<CacheKey, TypeCodec<?>>)
              notification ->
                  cacheRemovalListener.accept(notification.getKey(), notification.getValue()));
    }
    this.cache =
        cacheBuilder.build(
            new CacheLoader<CacheKey, TypeCodec<?>>() {
              @Override
              public TypeCodec<?> load(CacheKey key) throws Exception {
                return createCodec(key.cqlType, key.javaType);
              }
            });
  }

  public DefaultCodecRegistry(String logPrefix, TypeCodec<?>... userCodecs) {
    this(logPrefix, 0, null, 0, null, userCodecs);
  }

  @Override
  protected TypeCodec<?> getCachedCodec(DataType cqlType, GenericType<?> javaType) {
    LOG.trace("[{}] Checking cache", logPrefix);
    try {
      return cache.getUnchecked(new CacheKey(cqlType, javaType));
    } catch (UncheckedExecutionException | ExecutionError e) {
      // unwrap exception cause and throw it directly.
      Throwable cause = e.getCause();
      if (cause != null) {
        Throwables.throwIfUnchecked(cause);
        throw new DriverExecutionException(cause);
      } else {
        // Should never happen, throw just in case
        throw new RuntimeException(e.getMessage());
      }
    }
  }

  public static final class CacheKey {

    public final DataType cqlType;
    public final GenericType<?> javaType;

    public CacheKey(DataType cqlType, GenericType<?> javaType) {
      this.javaType = javaType;
      this.cqlType = cqlType;
    }

    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      } else if (other instanceof CacheKey) {
        CacheKey that = (CacheKey) other;
        return Objects.equals(this.cqlType, that.cqlType)
            && Objects.equals(this.javaType, that.javaType);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(cqlType, javaType);
    }
  }
}
