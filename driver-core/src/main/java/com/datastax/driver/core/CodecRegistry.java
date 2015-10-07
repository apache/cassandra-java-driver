/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Objects;
import com.google.common.cache.*;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

import com.datastax.driver.core.TypeCodec.*;
import com.datastax.driver.core.exceptions.CodecNotFoundException;

/**
 * A registry for {@link TypeCodec codec}s. When the driver
 * needs to serialize or deserialize an object,
 * it will lookup in the registry for
 * a suitable codec.
 *
 * <h3>Usage</h3>
 *
 * <p>
 * By default, the driver uses the CodecRegistry's {@link CodecRegistry#DEFAULT_INSTANCE},
 * a shareable instance that initially contains all the built-in codecs required by the driver.
 *
 * <p>
 * For most users, the default instance is good enough and
 * they won't need to manipulate {@link CodecRegistry} instances directly.
 * Only those willing to {@link #register(TypeCodec) register} user-defined codecs are required to do so:
 *
 * <pre>
 * CodecRegistry myCodecRegistry;
 * // use the default instance
 * myCodecRegistry = CodecRegistry.DEFAULT_INSTANCE;
 * // or alternatively, create a new one
 * myCodecRegistry = new CodecRegistry();
 * // then register additional codecs
 * myCodecRegistry.register(myCodec1, myCodec2, myCodec3);
 * </pre>
 *
 * <p>
 * To be used by the driver, {@link CodecRegistry} instances must then
 * be associated with a {@link Cluster} instance:
 *
 * <pre>
 * CodecRegistry myCodecRegistry = ...
 * Cluster cluster = Cluster.builder().withCodecRegistry(myCodecRegistry).build();
 * </pre>
 *
 * To retrieve the {@link CodecRegistry} instance associated with a Cluster, do the
 * following:
 *
 * <pre>
 * CodecRegistry registry = cluster.getConfiguration().getCodecRegistry();
 * </pre>
 *
 * <h3>Example</h3>
 *
 * <p>
 * Suppose that one wants to have all CQL timestamps seamlessly
 * converted to {@code java.time.DateTime} instances and vice versa; the following
 * code contains the necessary steps to achieve this:
 *
 * <pre>
 * // 1. Instantiate the custom codec
 * TypeCodec&lt;DateTime> timestampCodec = ...
 * // 2. Register the codec
 * CodecRegistry.DEFAULT_INSTANCE.register(timestampCodec);
 * </pre>
 *
 * <p>
 * Read the
 * <a href="http://datastax.github.io/java-driver/features/custom_codecs">online documentation</a>
 * for more examples.
 *
 * <h3>Notes</h3>
 *
 * <p>
 * <strong>Registration order</strong>: the order in which codecs are registered matters:
 * when looking for a suitable codec, the registry will consider
 * all registered codecs <em>in the order they were registered</em>, and
 * will pick the first matching one. If a codec is registered while another codec <em>already handles the same
 * CQL-to-Java mapping</em>, that codec will <em>override</em> the previously-registered one;
 * the driver will log a warning in these situations. Overriding registered codecs,
 * and specially the built-in ones, should rarely be justified.
 *
 * <p>
 * <strong>On-the-fly codec instantiation</strong>: When a {@link CodecRegistry}
 * cannot find a suitable codec among all registered codecs,
 * it will attempt to create such a codec on-the-fly, using the {@link CodecFactory}
 * provided at instantiation time. By default, the registry uses CodecFactory's {@link CodecFactory#DEFAULT_INSTANCE}.
 * If the factory is unable to create the codec, or if the created codec is not suitable,
 * then a {@link CodecNotFoundException} is thrown.
 *
 * <p>
 * <strong>Debugging the codec registry</strong>: it is possible to turn on log messages
 * by setting the {@code com.datastax.driver.core.CodecRegistry} logger level to {@code TRACE}.
 * Beware that the registry can be very verbose at this log level.
 *
 * <p>
 * <strong>Custom CQL types</strong>: Note that the default set of codecs has no support for
 * <a href="https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/db/marshal/AbstractType.java">custom CQL types</a>;
 * to be able to deserialize values of such types, you need to manually register an appropriate codec.
 *
 * <p>
 * <strong>Thread-safety</strong>: {@link CodecRegistry} instances are thread-safe.
 *
 */
public final class CodecRegistry {

    private static final Logger logger = LoggerFactory.getLogger(CodecRegistry.class);

    private static final ImmutableSet<TypeCodec<?>> PRIMITIVE_CODECS = ImmutableSet.of(
        BlobCodec.instance,
        BooleanCodec.instance,
        SmallIntCodec.instance,
        TinyIntCodec.instance,
        IntCodec.instance,
        BigintCodec.instance,
        CounterCodec.instance,
        DoubleCodec.instance,
        FloatCodec.instance,
        VarintCodec.instance,
        DecimalCodec.instance,
        VarcharCodec.instance, // must be declared before AsciiCodec so it gets chosen when CQL type not available
        AsciiCodec.instance,
        TimestampCodec.instance,
        DateCodec.instance,
        TimeCodec.instance,
        UUIDCodec.instance, // must be declared before TimeUUIDCodec so it gets chosen when CQL type not available
        TimeUUIDCodec.instance,
        InetCodec.instance
    );

    /**
     * A default instance of CodecRegistry.
     * For most applications, sharing a single instance of CodecRegistry is fine.
     * But note that any codec registered with this instance will immediately
     * be available for all its clients.
     */
    public static final CodecRegistry DEFAULT_INSTANCE = new CodecRegistry();

    /**
     * Cache key for the codecs cache.
     */
    private static final class TypeCodecCacheKey {

        private final DataType cqlType;

        private final TypeToken<?> javaType;

        public TypeCodecCacheKey(DataType cqlType, TypeToken<?> javaType) {
            this.javaType = javaType;
            this.cqlType = cqlType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            TypeCodecCacheKey cacheKey = (TypeCodecCacheKey)o;
            return Objects.equal(cqlType, cacheKey.cqlType) && Objects.equal(javaType, cacheKey.javaType);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(cqlType, javaType);
        }

    }

    /**
     * Cache loader for the codecs cache.
     */
    private class TypeCodecCacheLoader extends CacheLoader<TypeCodecCacheKey, TypeCodec<?>> {
        public TypeCodec<?> load(TypeCodecCacheKey cacheKey) {
            return findCodec(cacheKey.cqlType, cacheKey.javaType);
        }
    }

    /**
     * A complexity-based weigher for the codecs cache.
     * Weights are computed mainly according to the CQL type:
     * <ol>
     * <li>Manually-registered codecs always weigh 0;
     * <li>Codecs for primtive types weigh 0;
     * <li>Codecs for collections weigh the total weight of their inner types + the weight of their level of deepness;
     * <li>Codecs for UDTs and tuples weigh the total weight of their inner types + the weight of their level of deepness, but cannot weigh less than 1;
     * <li>Codecs for custom (non-CQL) types weigh 1.
     * </ol>
     * A consequence of this algorithm is that codecs for primitive types and codecs for all "shallow" collections thereof
     * are never evicted.
     */
    private class TypeCodecWeigher implements Weigher<TypeCodecCacheKey, TypeCodec<?>> {

        @Override
        public int weigh(TypeCodecCacheKey key, TypeCodec<?> value) {
            return codecs.contains(value) ? 0 : weigh(key.cqlType, 0);
        }

        private int weigh(DataType cqlType, int level) {
            switch (cqlType.getName()) {
                case LIST:
                case SET:
                case MAP: {
                    int weight = level;
                    for (DataType eltType : cqlType.getTypeArguments()) {
                        weight += weigh(eltType, level + 1);
                    }
                    return weight;
                }
                case UDT: {
                    int weight = level;
                    for (UserType.Field field : ((UserType)cqlType)) {
                        weight += weigh(field.getType(), level + 1);
                    }
                    return weight == 0 ? 1 : weight;
                }
                case TUPLE: {
                    int weight = level;
                    for (DataType componentType : ((TupleType)cqlType).getComponentTypes()) {
                        weight += weigh(componentType, level + 1);
                    }
                    return weight == 0 ? 1 : weight;
                }
                case CUSTOM:
                    return 1;
                default:
                    return 0;
            }
        }
    }

    /**
     * Simple removal listener for the codec cache (can be used for debugging purposes
     * by setting the {@code com.datastax.driver.core.CodecRegistry} logger level to {@code TRACE}.
     */
    private class TypeCodecRemovalListener implements RemovalListener<TypeCodecCacheKey, TypeCodec<?>> {
        @Override
        public void onRemoval(RemovalNotification<TypeCodecCacheKey, TypeCodec<?>> notification) {
            logger.trace("Evicting codec from cache: {} (cause: {})", notification.getValue(), notification.getCause());
        }
    }

    /**
     * The list of "initial" codecs.
     * This list is initialized with the built-in codecs;
     * User-defined codecs are appended to the list.
     */
    private final CopyOnWriteArrayList<TypeCodec<?>> codecs;

    /**
     * A LoadingCache to serve requests for codecs whenever possible.
     * The cache can be used as long as at least the CQL type is known.
     */
    private final LoadingCache<TypeCodecCacheKey, TypeCodec<?>> cache;

    /**
     * The factory to create codecs on-the-fly.
     */
    private final CodecFactory codecFactory;

    /**
     * Creates a default CodecRegistry instance with default cache options
     * and the default codec factory.
     */
    public CodecRegistry() {
        this(CodecFactory.DEFAULT_INSTANCE);
    }

    /**
     * Creates a default CodecRegistry instance with default cache options
     * and the provided codec factory.
     */
    public CodecRegistry(CodecFactory codecFactory) {
        this.codecs = new CopyOnWriteArrayList<TypeCodec<?>>(PRIMITIVE_CODECS);
        this.cache = defaultCacheBuilder().build(new TypeCodecCacheLoader());
        this.codecFactory = codecFactory;
    }

    private CacheBuilder<TypeCodecCacheKey, TypeCodec<?>> defaultCacheBuilder() {
        CacheBuilder<TypeCodecCacheKey, TypeCodec<?>> builder = CacheBuilder.newBuilder()
            // 19 primitive codecs + collections thereof = 19*3 + 19*19 = 418 codecs,
            // so let's start with roughly 1/4 of that
            .initialCapacity(100)
            .weigher(new TypeCodecWeigher())
            .maximumWeight(1000);
        if (logger.isTraceEnabled())
            // do not bother adding a listener if it will be ineffective
            builder = builder.removalListener(new TypeCodecRemovalListener());
        return builder;
    }

    /**
     * Register the given codec with this registry.
     *
     * @param codec The codec to add to the registry.
     * @return this CodecRegistry (for method chaining).
     */
    public CodecRegistry register(TypeCodec<?> codec) {
        return register(Collections.singleton(codec));
    }

    /**
     * Register the given codecs with this registry.
     *
     * @param codecs The codecs to add to the registry.
     * @return this Builder (for method chaining).
     */
    public CodecRegistry register(TypeCodec<?>... codecs) {
        return register(Arrays.asList(codecs));
    }

    /**
     * Register the given codecs with this registry.
     *
     * @param codecs The codecs to add to the registry.
     * @return this Builder (for method chaining).
     */
    public CodecRegistry register(Iterable<? extends TypeCodec<?>> codecs) {
        // protect against the check-then-act idiom;
        // a synchronized block is enough since this method
        // is not meant to be called very often,
        // and this is the only place where the collection is updated
        synchronized (this.codecs) {
            for (TypeCodec<?> codec : codecs) {
                int i = indexOf(codec, this.codecs);
                if (i == -1) {
                    this.codecs.add(codec);
                } else {
                    // new codec is overriding existing codec
                    TypeCodec<?> old = this.codecs.set(i, codec);
                    logger.warn("{} is overriding {}", codec, old);
                }
            }
        }
        return this;
    }

    /**
     * Check for a codec handling the same CQL-to-Java mapping.
     */
    private static int indexOf(TypeCodec<?> codec, List<TypeCodec<?>> codecs) {
        for (int i = 0; i < codecs.size(); i++) {
            TypeCodec<?> old = codecs.get(i);
            if(old.getCqlType().equals(codec.getCqlType()) && old.getJavaType().equals(codec.getJavaType())) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Returns a {@link TypeCodec codec} that accepts the given value.
     * <p>
     * This method takes an actual Java object and tries to locate a suitable codec for it.
     * For this reason, codecs must perform a {@link TypeCodec#accepts(Object) "manual" inspection}
     * of the object to determine if they can accept it or not, which, depending on the implementations,
     * can be an expensive operation; besides, the resulting codec cannot be cached.
     * Therefore there might be a performance penalty when using this method.
     * <p>
     * Furthermore, this method would return the first matching codec, regardless of its accepted {@link DataType CQL type}.
     * For this reason, this method might not return the most accurate codec and should be
     * reserved for situations where the target CQL type is not available or unknown.
     * In the Java driver, this happens mainly when serializing a value in a {@link SimpleStatement}
     * or in the {@link com.datastax.driver.core.querybuilder.QueryBuilder}, where no CQL type information
     * is available.
     * <p>
     * Regular application code should avoid using this method.
     * <p>
     * Codecs returned by this method are <em>NOT</em> cached.
     *
     * @param value The value the codec should accept; must not be {@code null}.
     * @return A suitable codec.
     * @throws CodecNotFoundException if a suitable codec cannot be found.
     */
    public <T> TypeCodec<T> codecFor(T value) {
        return findCodec(null, value);
    }

    /**
     * Returns a {@link TypeCodec codec} that accepts the given {@link DataType CQL type}.
     * <p>
     * This method would return the first matching codec, regardless of its accepted Java type.
     * For this reason, this method might not return the most accurate codec and should be
     * reserved for situations where the runtime type is not available or unknown.
     * In the Java driver, this happens mainly when deserializing a value using the
     * {@link AbstractGettableData#getObject(int)} method.
     * <p>
     * Codecs returned by this method are cached.
     *
     * @param cqlType The {@link DataType CQL type} the codec should accept; must not be {@code null}.
     * @return A suitable codec.
     * @throws CodecNotFoundException if a suitable codec cannot be found.
     */
    public <T> TypeCodec<T> codecFor(DataType cqlType) throws CodecNotFoundException {
        return lookupCodec(cqlType, null);
    }

    /**
     * Returns a {@link TypeCodec codec} that accepts the given {@link DataType CQL type}
     * and the given Java class.
     * <p>
     * This method can only handle raw (non-parameterized) Java types.
     * For parameterized types, use {@link #codecFor(DataType, TypeToken)} instead.
     * <p>
     * Note that type inheritance needs special care.
     * If a codec accepts a Java type that is assignable to the
     * given Java type, that codec may be returned if it is found first
     * in the registry, <em>even if another codec is a better match</em>.
     * <p>
     * Codecs returned by this method are cached.
     *
     * @param cqlType The {@link DataType CQL type} the codec should accept; must not be {@code null}.
     * @param javaType The Java type the codec should accept; can be {@code null}.
     * @return A suitable codec.
     * @throws CodecNotFoundException if a suitable codec cannot be found.
     */
    public <T> TypeCodec<T> codecFor(DataType cqlType, Class<T> javaType) throws CodecNotFoundException {
        return codecFor(cqlType, TypeToken.of(javaType));
    }

    /**
     * Returns a {@link TypeCodec codec} that accepts the given {@link DataType CQL type}
     * and the given Java type.
     * <p>
     * This method handles parameterized types thanks to Guava's {@link TypeToken} API.
     * <p>
     * Note that type inheritance needs special care.
     * If a codec accepts a Java type that is assignable to the
     * given Java type, that codec may be returned if it is found first
     * in the registry, <em>even if another codec is a better match</em>.
     * <p>
     * Codecs returned by this method are cached.
     *
     * @param cqlType The {@link DataType CQL type} the codec should accept; must not be {@code null}.
     * @param javaType The {@link TypeToken Java type} the codec should accept; can be {@code null}.
     * @return A suitable codec.
     * @throws CodecNotFoundException if a suitable codec cannot be found.
     */
    public <T> TypeCodec<T> codecFor(DataType cqlType, TypeToken<T> javaType) throws CodecNotFoundException {
        return lookupCodec(cqlType, javaType);
    }

    /**
     * Returns a {@link TypeCodec codec} that accepts the given {@link DataType CQL type}
     * and the given value.
     * <p>
     * This method takes an actual Java object and tries to locate a suitable codec for it.
     * For this reason, codecs must perform a {@link TypeCodec#accepts(Object) "manual" inspection}
     * of the object to determine if they can accept it or not, which, depending on the implementations,
     * can be an expensive operation; besides, the resulting codec cannot be cached.
     * Therefore there might be a performance penalty when using this method.
     * <p>
     * Codecs returned by this method are <em>NOT</em> cached.
     *
     * @param cqlType The {@link DataType CQL type} the codec should accept; can be {@code null}.
     * @param value The value the codec should accept; must not be {@code null}.
     * @return A suitable codec.
     * @throws CodecNotFoundException if a suitable codec cannot be found.
     */
    public <T> TypeCodec<T> codecFor(DataType cqlType, T value) {
        return findCodec(cqlType, value);
    }

    @SuppressWarnings("unchecked")
    private <T> TypeCodec<T> lookupCodec(DataType cqlType, TypeToken<T> javaType) {
        checkNotNull(cqlType, "Parameter cqlType cannot be null");
        logger.trace("Querying cache for codec [{} <-> {}]", cqlType, javaType == null ? "ANY" : javaType);
        TypeCodecCacheKey cacheKey = new TypeCodecCacheKey(cqlType, javaType);
        try {
            TypeCodec<?> codec = cache.get(cacheKey);
            logger.trace("Returning cached codec {}", codec);
            return (TypeCodec<T>)codec;
        } catch (UncheckedExecutionException e) {
            if (e.getCause() instanceof CodecNotFoundException) {
                throw (CodecNotFoundException)e.getCause();
            }
            throw new CodecNotFoundException(e.getCause(), cqlType, javaType);
        } catch (ExecutionException e) {
            throw new CodecNotFoundException(e.getCause(), cqlType, javaType);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> TypeCodec<T> findCodec(DataType cqlType, TypeToken<T> javaType) {
        checkNotNull(cqlType, "Parameter cqlType cannot be null");
        logger.trace("Looking for codec [{} <-> {}]", cqlType, javaType == null ? "ANY" : javaType);
        for (TypeCodec<?> codec : codecs) {
            if (codec.accepts(cqlType) && (javaType == null || codec.accepts(javaType))) {
                logger.trace("Codec found: {}", codec);
                return (TypeCodec<T>)codec;
            }
        }
        return createCodec(cqlType, javaType);
    }

    @SuppressWarnings("unchecked")
    private <T> TypeCodec<T> findCodec(DataType cqlType, T value) {
        checkNotNull(value, "Parameter value cannot be null");
        logger.trace("Looking for codec [{} <-> {}]", cqlType == null ? "ANY" : cqlType, value.getClass());
        for (TypeCodec<?> codec : codecs) {
            if ((cqlType == null || codec.accepts(cqlType)) && codec.accepts(value)) {
                logger.trace("Codec found: {}", codec);
                return (TypeCodec<T>)codec;
            }
        }
        return createCodec(cqlType, value);
    }

    private <T> TypeCodec<T> createCodec(DataType cqlType, TypeToken<T> javaType) {
        TypeCodec<T> codec = codecFactory.newCodec(cqlType, javaType, this);
        if (codec == null)
            throw notFound(cqlType, javaType);
        // double-check that the created codec satisfies the initial request
        if (!codec.accepts(cqlType) || (javaType != null && !codec.accepts(javaType)))
            throw notFound(cqlType, javaType);
        logger.trace("Codec created: {}", codec);
        return codec;
    }

    private <T> TypeCodec<T> createCodec(DataType cqlType, T value) {
        TypeCodec<T> codec = codecFactory.newCodec(cqlType, value, this);
        if (codec == null)
            throw notFound(cqlType, TypeToken.of(value.getClass()));
        // double-check that the created codec satisfies the initial request
        if ((cqlType != null && !codec.accepts(cqlType)) || !codec.accepts(value))
            throw notFound(cqlType, TypeToken.of(value.getClass()));
        logger.trace("Codec created: {}", codec);
        return codec;
    }

    private CodecNotFoundException notFound(DataType cqlType, TypeToken<?> javaType) {
        String msg = String.format("Codec not found for requested operation: [%s <-> %s]",
            cqlType == null ? "ANY" : cqlType,
            javaType == null ? "ANY" : javaType);
        return new CodecNotFoundException(msg, cqlType, javaType);
    }

}
