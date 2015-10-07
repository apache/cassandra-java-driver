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

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Objects;
import com.google.common.cache.*;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

import com.datastax.driver.core.TypeCodec.*;
import com.datastax.driver.core.exceptions.CodecNotFoundException;

import static com.datastax.driver.core.DataType.Name.*;

/**
 * A registry for {@link TypeCodec}s. When the driver
 * needs to serialize or deserialize an object,
 * it will lookup in the {@link CodecRegistry} for
 * a suitable codec.
 *
 * <h3>Usage</h3>
 *
 * <p>
 * Most users won't need to manipulate {@link CodecRegistry} instances directly.
 * Only those willing to register user-defined codecs are required to do so.
 * By default, the driver uses {@link CodecRegistry#DEFAULT_INSTANCE}, a shareable
 * instance that initially contains all the built-in codecs required by the driver.
 *
 * <p>
 * Users willing to customize their {@link CodecRegistry} can do so by {@code register registering}
 * new {@link TypeCodec}s either on {@link CodecRegistry#DEFAULT_INSTANCE},
 * or on a newly-crated one:
 *
 * <pre>
 * CodecRegistry myCodecRegistry;
 * // use the default instance
 * myCodecRegistry = CodecRegistry.DEFAULT_INSTANCE;
 * // or alternatively, create a new one
 * myCodecRegistry = new CodecRegistry();
 * // then
 * myCodecRegistry.register(myCodec1, myCodec2, myCodec3);
 * </pre>
 *
 * <p>
 * Note that the order in which codecs are registered matters;
 * if a codec is registered while another codec already handles the same
 * CQL-to-Java mapping, that codec will never be used (unless
 * the user forces its usage through e.g. {@link GettableByIndexData#get(int, TypeCodec)}).
 * In order words, it is not possible to replace an existing codec,
 * specially the built-in ones; it is only possible to enrich the initial
 * set of codecs with new ones.
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
 * <p>
 * By default, {@link Cluster} instances will use {@link CodecRegistry#DEFAULT_INSTANCE}.
 *
 * <h3>Example</h3>
 *
 * <p>
 * E.g. let's suppose you want to have all your CQL timestamps
 * deserialized as {@code java.time.DateTime} instances:
 *
 * <pre>
 * TypeCodec&lt;DateTime> timestampCodec = ...
 * CodecRegistry myCodecRegistry = new CodecRegistry().register(timestampCodec);
 * </pre>
 *
 * Read the
 * <a href="http://datastax.github.io/java-driver/features/custom_codecs">online documentation</a>
 * for more examples.
 *
 * <h3>Notes</h3>
 *
 * <p>
 * When a {@link CodecRegistry} cannot find a suitable codec among all registered codecs,
 * it will attempt to create a suitable codec.
 * <p>
 * If the creation succeeds, that codec is added to the list of known codecs and is returned;
 * otherwise, a {@link CodecNotFoundException} is thrown.
 * <p>
 * Note that {@link CodecRegistry} instances can only create codecs in very limited situations:
 * <ol>
 *     <li>Codecs for {@link UserType user types} are created on the fly using  {@link UDTCodec};</li>
 *     <li>Codecs for {@link TupleType tuple types} are created on the fly using  {@link TupleCodec};</li>
 *     <li>Codecs for collections are created on the fly using {@link ListCodec}, {@link SetCodec} and
 *     {@link MapCodec}, if their element types can be handled by existing codecs (or codecs that can
 *     themselves be generated).</li>
 * </ol>
 * Other combinations of Java and CQL types cannot have their codecs created on the fly;
 * such codecs must be manually registered.
 * <p>
 * Note that the default set of codecs has no support for
 * <a href="https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/db/marshal/AbstractType.java">Cassandra custom types</a>;
 * to be able to deserialize values of such types, you need to manually register an appropriate codec.
 * <p>
 * {@link CodecRegistry} instances are not meant to be shared between two or more {@link Cluster} instances; doing so
 * could yield unexpected results.
 * <p>
 * {@link CodecRegistry} instances are thread-safe.
 *
 */
public final class CodecRegistry {

    private static final Logger logger = LoggerFactory.getLogger(CodecRegistry.class);

    @SuppressWarnings("unchecked")
    private static final ImmutableSet<TypeCodec<?>> PRIMITIVE_CODECS = ImmutableSet.of(
        TypeCodec.blobCodec(),
        TypeCodec.booleanCodec(),
        TypeCodec.smallIntCodec(),
        TypeCodec.tinyIntCodec(),
        TypeCodec.intCodec(),
        TypeCodec.bigintCodec(),
        TypeCodec.counterCodec(),
        TypeCodec.doubleCodec(),
        TypeCodec.floatCodec(),
        TypeCodec.varintCodec(),
        TypeCodec.decimalCodec(),
        TypeCodec.varcharCodec(), // must be declared before AsciiCodec so it gets chosen when CQL type not available
        TypeCodec.asciiCodec(),
        TypeCodec.timestampCodec(),
        TypeCodec.dateCodec(),
        TypeCodec.timeCodec(),
        TypeCodec.uuidCodec(), // must be declared before TimeUUIDCodec so it gets chosen when CQL type not available
        TypeCodec.timeUUIDCodec(),
        TypeCodec.inetCodec()
    );

    /**
     * A default instance of CodecRegistry.
     * For most applications, sharing a single instance of CodecRegistry is fine.
     * But note that any codec registered with this instance will immediately
     * be available for all its clients.
     */
    public static final CodecRegistry DEFAULT_INSTANCE = new CodecRegistry();

    private static final class CacheKey {

        private final DataType cqlType;

        private final TypeToken<?> javaType;

        public CacheKey(DataType cqlType, TypeToken<?> javaType) {
            this.javaType = javaType;
            this.cqlType = cqlType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            CacheKey cacheKey = (CacheKey)o;
            return Objects.equal(cqlType, cacheKey.cqlType) && Objects.equal(javaType, cacheKey.javaType);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(javaType, cqlType);
        }
    }

    /**
     * A complexity-based weigher.
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
    private class TypeCodecWeigher implements Weigher<CacheKey, TypeCodec<?>> {

        @Override
        public int weigh(CacheKey key, TypeCodec<?> value) {
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

    private class TypeCodecRemovalListener implements RemovalListener<CacheKey, TypeCodec<?>> {
        @Override
        public void onRemoval(RemovalNotification<CacheKey, TypeCodec<?>> notification) {
            logger.trace("Evicting codec from cache: {} (cause: {})", notification.getValue(), notification.getCause());
        }
    }

    /**
     * The list of all known codecs.
     * This list is initialized with the built-in codecs;
     * User-defined codecs are appended to the list.
     */
    private final CopyOnWriteArrayList<TypeCodec<?>> codecs;

    /**
     * A LoadingCache to serve requests for codecs whenever possible.
     * The cache can be used as long as at least the CQL type is known.
     */
    private final LoadingCache<CacheKey, TypeCodec<?>> cache;

    /**
     * Creates a default CodecRegistry instance with default cache options.
     */
    public CodecRegistry() {
        this.codecs = new CopyOnWriteArrayList<TypeCodec<?>>(PRIMITIVE_CODECS);
        this.cache = CacheBuilder.newBuilder()
            // 19 primitive codecs + collections thereof = 19*3 + 19*19 = 418 codecs,
            // so let's start with roughly 1/4 of that
            .initialCapacity(100)
            .weigher(new TypeCodecWeigher())
            // a cache with all 418 "basic" codecs weighs 399 (19*2 + 19*19),
            // so let's cap at roughly 2.5x this size
            .maximumWeight(1000)
            .concurrencyLevel(Runtime.getRuntime().availableProcessors() * 4)
            .removalListener(new TypeCodecRemovalListener())
            .build(
                new CacheLoader<CacheKey, TypeCodec<?>>() {
                    public TypeCodec<?> load(CacheKey cacheKey) {
                        return findCodec(cacheKey.cqlType, cacheKey.javaType);
                    }
                });
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
        for (TypeCodec<?> codec : codecs) {
            this.codecs.add(codec);
        }
        return this;
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
        checkNotNull(value, "Parameter value cannot be null");
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
     * @param javaType The Java type the codec should accept; must not be {@code null}.
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
     * @param javaType The {@link TypeToken Java type} the codec should accept; must not be {@code null}.
     * @return A suitable codec.
     * @throws CodecNotFoundException if a suitable codec cannot be found.
     */
    public <T> TypeCodec<T> codecFor(DataType cqlType, TypeToken<T> javaType) throws CodecNotFoundException {
        checkNotNull(cqlType, "Parameter cqlType cannot be null");
        checkNotNull(javaType, "Parameter javaType cannot be null");
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
     * @param cqlType The {@link DataType CQL type} the codec should accept; must not be {@code null}.
     * @param value The value the codec should accept; must not be {@code null}.
     * @return A suitable codec.
     * @throws CodecNotFoundException if a suitable codec cannot be found.
     */
    public <T> TypeCodec<T> codecFor(DataType cqlType, T value) {
        checkNotNull(cqlType, "Parameter cqlType cannot be null");
        checkNotNull(value, "Parameter value cannot be null");
        return findCodec(cqlType, value);
    }

    @SuppressWarnings("unchecked")
    private <T> TypeCodec<T> lookupCodec(DataType cqlType, TypeToken<T> javaType) {
        checkNotNull(cqlType, "Parameter cqlType cannot be null");
        logger.trace("Querying cache for codec [{} <-> {}]", cqlType, javaType);
        CacheKey cacheKey = new CacheKey(cqlType, javaType);
        try {
            TypeCodec<?> codec = cache.get(cacheKey);
            logger.trace("Returning cached codec [{} <-> {}]", cqlType, javaType);
            return (TypeCodec<T>)codec;
        } catch (UncheckedExecutionException e) {
            if(e.getCause() instanceof CodecNotFoundException) {
                throw (CodecNotFoundException) e.getCause();
            }
            throw new CodecNotFoundException(e.getCause(), cqlType, javaType);
        } catch (ExecutionException e) {
            throw new CodecNotFoundException(e.getCause(), cqlType, javaType);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> TypeCodec<T> findCodec(DataType cqlType, TypeToken<T> javaType) {
        checkNotNull(cqlType, "Parameter cqlType cannot be null");
        logger.trace("Looking for codec [{} <-> {}]",
            cqlType,
            javaType == null ? "ANY" : javaType);
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
        TypeCodec<T> codec = maybeCreateCodec(cqlType, javaType);
        if (codec == null)
            throw newException(cqlType, javaType);
        // double-check that the created codec satisfies the initial request
        if (!codec.accepts(cqlType) || (javaType != null && !codec.accepts(javaType)))
            throw newException(cqlType, javaType);
        logger.trace("Codec created: {}", codec);
        return codec;
    }

    private <T> TypeCodec<T> createCodec(DataType cqlType, T value) {
        TypeCodec<T> codec = maybeCreateCodec(cqlType, value);
        if(codec == null)
            throw newException(cqlType, TypeToken.of(value.getClass()));
        // double-check that the created codec satisfies the initial request
        if((cqlType != null && !codec.accepts(cqlType)) || !codec.accepts(value))
            throw newException(cqlType, TypeToken.of(value.getClass()));
        logger.trace("Codec created: {}", codec);
        return codec;
    }

    @SuppressWarnings("unchecked")
    private <T> TypeCodec<T> maybeCreateCodec(DataType cqlType, TypeToken<T> javaType) {
        checkNotNull(cqlType);

        if (cqlType.getName() == LIST && (javaType == null || List.class.isAssignableFrom(javaType.getRawType()))) {
            TypeToken<?> elementType = null;
            if (javaType != null && javaType.getType() instanceof ParameterizedType) {
                Type[] typeArguments = ((ParameterizedType)javaType.getType()).getActualTypeArguments();
                elementType = TypeToken.of(typeArguments[0]);
            }
            TypeCodec<?> eltCodec = findCodec(cqlType.getTypeArguments().get(0), elementType);
            return (TypeCodec<T>)TypeCodec.listCodec(eltCodec);
        }

        if (cqlType.getName() == SET && (javaType == null || Set.class.isAssignableFrom(javaType.getRawType()))) {
            TypeToken<?> elementType = null;
            if (javaType != null && javaType.getType() instanceof ParameterizedType) {
                Type[] typeArguments = ((ParameterizedType)javaType.getType()).getActualTypeArguments();
                elementType = TypeToken.of(typeArguments[0]);
            }
            TypeCodec<?> eltCodec = findCodec(cqlType.getTypeArguments().get(0), elementType);
            return (TypeCodec<T>)TypeCodec.setCodec(eltCodec);
        }

        if (cqlType.getName() == MAP && (javaType == null || Map.class.isAssignableFrom(javaType.getRawType()))) {
            TypeToken<?> keyType = null;
            TypeToken<?> valueType = null;
            if (javaType != null && javaType.getType() instanceof ParameterizedType) {
                Type[] typeArguments = ((ParameterizedType)javaType.getType()).getActualTypeArguments();
                keyType = TypeToken.of(typeArguments[0]);
                valueType = TypeToken.of(typeArguments[1]);
            }
            TypeCodec<?> keyCodec = findCodec(cqlType.getTypeArguments().get(0), keyType);
            TypeCodec<?> valueCodec = findCodec(cqlType.getTypeArguments().get(1), valueType);
            return (TypeCodec<T>)TypeCodec.mapCodec(keyCodec, valueCodec);
        }

        if (cqlType instanceof TupleType && (javaType == null || TupleValue.class.isAssignableFrom(javaType.getRawType()))) {
            return (TypeCodec<T>)TypeCodec.tupleCodec((TupleType)cqlType);
        }

        if (cqlType instanceof UserType && (javaType == null || UDTValue.class.isAssignableFrom(javaType.getRawType()))) {
            return (TypeCodec<T>)TypeCodec.userTypeCodec((UserType)cqlType);
        }

        return null;
    }

    @SuppressWarnings("unchecked")
    private <T> TypeCodec<T> maybeCreateCodec(DataType cqlType, T value) {
        checkNotNull(value);

        if ((cqlType == null || cqlType.getName() == LIST) && value instanceof List) {
            List list = (List)value;
            if (list.isEmpty()) {
                DataType elementType = (cqlType == null || cqlType.getTypeArguments().isEmpty())
                    ? DataType.blob()
                    : cqlType.getTypeArguments().get(0);
                return TypeCodec.listCodec(findCodec(elementType, (TypeToken)null));
            } else {
                DataType elementType = (cqlType == null || cqlType.getTypeArguments().isEmpty())
                    ? null
                    : cqlType.getTypeArguments().get(0);
                return (TypeCodec<T>)TypeCodec.listCodec(findCodec(elementType, list.iterator().next()));
            }
        }

        if ((cqlType == null || cqlType.getName() == SET) && value instanceof Set) {
            Set set = (Set)value;
            if (set.isEmpty()) {
                DataType elementType = (cqlType == null || cqlType.getTypeArguments().isEmpty())
                    ? DataType.blob()
                    : cqlType.getTypeArguments().get(0);
                return TypeCodec.setCodec(findCodec(elementType, (TypeToken)null));
            } else {
                DataType elementType = (cqlType == null || cqlType.getTypeArguments().isEmpty())
                    ? null
                    : cqlType.getTypeArguments().get(0);
                return (TypeCodec<T>)TypeCodec.setCodec(findCodec(elementType, set.iterator().next()));
            }
        }

        if ((cqlType == null || cqlType.getName() == MAP) && value instanceof Map) {
            Map map = (Map)value;
            if (map.isEmpty()) {
                DataType keyType = (cqlType == null || cqlType.getTypeArguments().size() < 1)
                    ? DataType.blob()
                    : cqlType.getTypeArguments().get(0);
                DataType valueType = (cqlType == null || cqlType.getTypeArguments().size() < 2)
                    ? DataType.blob() :
                    cqlType.getTypeArguments().get(1);
                return TypeCodec.mapCodec(
                    findCodec(keyType, (TypeToken)null),
                    findCodec(valueType, (TypeToken)null));
            } else {
                DataType keyType = (cqlType == null || cqlType.getTypeArguments().size() < 1)
                    ? null
                    : cqlType.getTypeArguments().get(0);
                DataType valueType = (cqlType == null || cqlType.getTypeArguments().size() < 2)
                    ? null
                    : cqlType.getTypeArguments().get(1);
                Map.Entry entry = (Map.Entry)map.entrySet().iterator().next();
                return (TypeCodec<T>)TypeCodec.mapCodec(
                    findCodec(keyType, entry.getKey()),
                    findCodec(valueType, entry.getValue()));
            }
        }

        if ((cqlType == null || cqlType.getName() == DataType.Name.TUPLE) && value instanceof TupleValue) {
            return (TypeCodec<T>)TypeCodec.tupleCodec(cqlType == null ? ((TupleValue)value).getType() : (TupleType)cqlType);
        }

        if ((cqlType == null || cqlType.getName() == DataType.Name.UDT) && value instanceof UDTValue) {
            return (TypeCodec<T>)TypeCodec.userTypeCodec(cqlType == null ? ((UDTValue)value).getType() : (UserType)cqlType);
        }

        return null;
    }

    private static CodecNotFoundException newException(DataType cqlType, TypeToken<?> javaType) {
        String msg = String.format("Codec not found for requested operation: [%s <-> %s]",
            cqlType == null ? "ANY" : cqlType,
            javaType == null ? "ANY" : javaType);
        return new CodecNotFoundException(msg, cqlType, javaType);
    }

}
