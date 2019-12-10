/*
 * Copyright DataStax, Inc.
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
import com.datastax.oss.driver.shaded.guava.common.base.Throwables;
import com.datastax.oss.driver.shaded.guava.common.cache.CacheBuilder;
import com.datastax.oss.driver.shaded.guava.common.cache.CacheLoader;
import com.datastax.oss.driver.shaded.guava.common.cache.LoadingCache;
import com.datastax.oss.driver.shaded.guava.common.cache.RemovalListener;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.ExecutionError;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.UncheckedExecutionException;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default codec registry implementation.
 *
 * <p>It is a caching registry based on Guava cache (note that the driver shades Guava).
 */
@ThreadSafe
public class DefaultCodecRegistry extends CachingCodecRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultCodecRegistry.class);

  private final LoadingCache<CacheKey, TypeCodec<?>> cache;

  /**
   * Creates a new instance that accepts user codecs, with the default built-in codecs and the
   * default cache behavior.
   */
  public DefaultCodecRegistry(@NonNull String logPrefix) {
    this(logPrefix, CodecRegistryConstants.PRIMITIVE_CODECS);
  }

  /**
   * Creates a new instance that accepts user codecs, with the given built-in codecs and the default
   * cache behavior.
   */
  public DefaultCodecRegistry(@NonNull String logPrefix, @NonNull TypeCodec<?>... primitiveCodecs) {
    this(logPrefix, 0, null, 0, null, primitiveCodecs);
  }

  /**
   * Same as {@link #DefaultCodecRegistry(String, TypeCodec[])}, but with some amount of control
   * over cache behavior.
   *
   * <p>Giving full access to the Guava cache API would be too much work, since it is shaded and we
   * have to wrap everything. If you need something that's not available here, it's easy enough to
   * write your own CachingCodecRegistry implementation. It's doubtful that stuff like cache
   * eviction is that useful anyway.
   */
  public DefaultCodecRegistry(
      @NonNull String logPrefix,
      int initialCacheCapacity,
      @Nullable BiFunction<CacheKey, TypeCodec<?>, Integer> cacheWeigher,
      int maximumCacheWeight,
      @Nullable BiConsumer<CacheKey, TypeCodec<?>> cacheRemovalListener,
      @NonNull TypeCodec<?>... primitiveCodecs) {

    super(logPrefix, primitiveCodecs);
    CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
    if (initialCacheCapacity > 0) {
      cacheBuilder.initialCapacity(initialCacheCapacity);
    }
    if (cacheWeigher != null) {
      cacheBuilder.weigher(cacheWeigher::apply).maximumWeight(maximumCacheWeight);
    }
    CacheLoader<CacheKey, TypeCodec<?>> cacheLoader =
        new CacheLoader<CacheKey, TypeCodec<?>>() {
          @Override
          public TypeCodec<?> load(@NonNull CacheKey key) throws Exception {
            return createCodec(key.cqlType, key.javaType, key.isJavaCovariant);
          }
        };
    if (cacheRemovalListener != null) {
      this.cache =
          cacheBuilder
              .removalListener(
                  (RemovalListener<CacheKey, TypeCodec<?>>)
                      notification ->
                          cacheRemovalListener.accept(
                              notification.getKey(), notification.getValue()))
              .build(cacheLoader);
    } else {
      this.cache = cacheBuilder.build(cacheLoader);
    }
  }

  @Override
  protected TypeCodec<?> getCachedCodec(
      @Nullable DataType cqlType, @Nullable GenericType<?> javaType, boolean isJavaCovariant) {
    LOG.trace("[{}] Checking cache", logPrefix);
    try {
      return cache.getUnchecked(new CacheKey(cqlType, javaType, isJavaCovariant));
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
    public final boolean isJavaCovariant;

    public CacheKey(
        @Nullable DataType cqlType, @Nullable GenericType<?> javaType, boolean isJavaCovariant) {
      this.javaType = javaType;
      this.cqlType = cqlType;
      this.isJavaCovariant = isJavaCovariant;
    }

    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      } else if (other instanceof CacheKey) {
        CacheKey that = (CacheKey) other;
        return Objects.equals(this.cqlType, that.cqlType)
            && Objects.equals(this.javaType, that.javaType)
            && this.isJavaCovariant == that.isJavaCovariant;
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(cqlType, javaType, isJavaCovariant);
    }
  }
}
