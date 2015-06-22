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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
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
 * To create a default {@link CodecRegistry} instance with all the default codecs used
 * by the Java driver, simply use the following:
 *
 * <pre>
 * CodecRegistry myCodecRegistry = new CodecRegistry();
 * </pre>
 *
 * Custom {@link TypeCodec}s can be added to a {@link CodecRegistry} via the {@code register} methods:
 *
 * <pre>
 * CodecRegistry myCodecRegistry = new CodecRegistry()
 *    .register(myCodec1, myCodec2, myCodec3)
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
 * For most users, a default {@link CodecRegistry} instance, containing
 * all the default codecs, should be adequate.
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
 *     <li>Codecs for Enums are created on the fly using {@link EnumStringCodec};</li>
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
@SuppressWarnings("all")
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
     * The default set of codecs used by the Java driver.
     * They contain codecs to handle native types, and collections thereof (lists, sets and maps).
     *
     * Note that the default set of codecs has no support for
     * <a href="https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/db/marshal/AbstractType.java">Cassandra custom types</a>;
     * to be able to deserialize values of such types, you need to manually register an appropriate codec.
     */
    static final ImmutableList<TypeCodec<?>> DEFAULT_CODECS;

    static {
        ImmutableList.Builder<TypeCodec<?>> builder = new ImmutableList.Builder<TypeCodec<?>>();
        builder.addAll(PRIMITIVE_CODECS);
        for (TypeCodec<?> primitiveCodec1 : PRIMITIVE_CODECS) {
            builder.add(new ListCodec(primitiveCodec1));
            builder.add(new SetCodec(primitiveCodec1));
            for (TypeCodec<?> primitiveCodec2 : PRIMITIVE_CODECS) {
                builder.add(new MapCodec(primitiveCodec1, primitiveCodec2));
            }
        }
        DEFAULT_CODECS = builder.build();
    }

    /**
     * An immutable instance of CodecRegistry.
     * This is only meant to be used internally by the driver.
     */
    public static final CodecRegistry IMMUTABLE_INSTANCE = new CodecRegistry(true);

    private static final class CacheKey {

        private final TypeToken<?> javaType;

        private final DataType cqlType;

        public CacheKey(TypeToken<?> javaType, DataType cqlType) {
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
            return Objects.equal(javaType, cacheKey.javaType) && Objects.equal(cqlType, cacheKey.cqlType);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(javaType, cqlType);
        }
    }

    private final boolean immutable;

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

    public CodecRegistry() {
        this(false);
    }

    private CodecRegistry(boolean immutable) {
        this.immutable = immutable;
        this.codecs = new CopyOnWriteArrayList<TypeCodec<?>>(DEFAULT_CODECS);
        this.cache = CacheBuilder.newBuilder()
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
        return register(codec, false);
    }

    /**
     * Register the given codec with this registry.
     *
     * @param codec The codec to add to the registry.
     * @param includeDerivedCollectionCodecs If {@code true}, register not only the given codec
     * but also collection codecs whose element types are handled by the given codec, i.e.:
     * <ul>
     *   <li>lists of the codec's type;</li>
     *   <li>sets of the codec's type;</li>
     *   <li>maps of a primitive type to the codec's type, or of the codec's type to a primitive
     *   type;</li>
     * </ul>
     * @return this CodecRegistry (for method chaining).
     */
    public CodecRegistry register(TypeCodec<?> codec, boolean includeDerivedCollectionCodecs) {
        return register(Collections.singleton(codec), includeDerivedCollectionCodecs);
    }

    /**
     * Register the given codecs with this registry.
     *
     * @param codecs The codecs to add to the registry.
     * @return this Builder (for method chaining).
     */
    public CodecRegistry register(TypeCodec<?>... codecs) {
        return register(Arrays.asList(codecs), false);
    }

    /**
     * Register the given codecs with this registry.
     *
     * @param codecs The codecs to add to the registry.
     * @return this Builder (for method chaining).
     */
    public CodecRegistry register(Iterable<? extends TypeCodec<?>> codecs) {
        return register(codecs, false);
    }

    /**
     * Register the given codecs with this registry.
     *
     * @param codecs The codecs to add to the registry.
     * @param includeDerivedCollectionCodecs If {@code true}, register not only the given codecs
     * but also collection codecs whose element types are handled by the given codecs, i.e. for
     * each codec:
     * <ul>
     *   <li>lists of the codec's type;</li>
     *   <li>sets of the codec's type;</li>
     *   <li>maps of a primitive type to the codec's type, or of the codec's type to a primitive
     *   type;</li>
     * </ul>
     * @return this Builder (for method chaining).
     */
    public CodecRegistry register(Iterable<? extends TypeCodec<?>> codecs, boolean includeDerivedCollectionCodecs) {
        if(immutable)
            throw new UnsupportedOperationException("This CodecRegistry instance is immutable");
        for (TypeCodec<?> codec : codecs) {
            // add this codec to the beginning of the list so that it gets higher priority
            this.codecs.add(codec);
            if(includeDerivedCollectionCodecs){
                // list and set codecs of the given type
                this.codecs.add(new ListCodec(codec));
                this.codecs.add(new SetCodec(codec));
                // a map which keys and values are of the same given type
                this.codecs.add(new MapCodec(codec, codec));
                // map codecs for combinations of the given type with all primitive types
                for (TypeCodec<?> primitiveCodec : PRIMITIVE_CODECS) {
                    this.codecs.add(new MapCodec(primitiveCodec, codec));
                    this.codecs.add(new MapCodec(codec, primitiveCodec));
                }
            }
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
     * Codecs returned by this method are NOT cached.
     *
     * @param value The value the codec should accept; must not be {@code null}.
     * @return A suitable codec.
     * @throws CodecNotFoundException if a suitable codec cannot be found.
     */
    public <T> TypeCodec<T> codecFor(T value) {
        checkNotNull(value, "Parameter value cannot be null");
        TypeCodec<T> codec = findCodec(null, value);
        return codec;
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
        return lookupCache(cqlType, null);
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
        return lookupCache(cqlType, javaType);
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
     * Codecs returned by this method <em>may</em> be cached.
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

    private <T> TypeCodec<T> lookupCache(DataType cqlType, TypeToken<T> javaType) {
        checkNotNull(cqlType, "Parameter cqlType cannot be null");
        if(logger.isTraceEnabled())
            logger.trace("Looking up codec for {} <-> {}", cqlType, javaType);
        CacheKey cacheKey = new CacheKey(javaType, cqlType);
        try {
            return (TypeCodec<T>)cache.get(cacheKey);
        } catch (UncheckedExecutionException e) {
            if(e.getCause() instanceof CodecNotFoundException)
                throw (CodecNotFoundException)e.getCause();
            throw new CodecNotFoundException(e.getCause(), cqlType, javaType);
        } catch (ExecutionException e) {
            throw new CodecNotFoundException(e.getCause(), cqlType, javaType);
        }
    }

    private <T> TypeCodec<T> findCodec(DataType cqlType, TypeToken<T> javaType) {
        checkNotNull(cqlType, "Parameter cqlType cannot be null");
        if(logger.isTraceEnabled())
            logger.trace("Looking for codec [{} <-> {}]", cqlType == null ? "ANY" : cqlType, javaType == null ? "ANY" : javaType);
        for (TypeCodec<?> codec : codecs) {
            if ((cqlType == null || codec.accepts(cqlType)) && (javaType == null || codec.accepts(javaType))) {
                if(logger.isTraceEnabled())
                    logger.trace("Codec found: {}", codec);
                return (TypeCodec<T>)codec;
            }
        }
        // codec not found among the reservoir of known codecs, try to create an ad-hoc codec;
        // this situation should happen mainly when dealing with enums, tuples and UDTs,
        // and collections thereof.
        TypeCodec<T> codec = maybeCreateCodec(cqlType, javaType);
        if (codec == null)
            throw logErrorAndThrow(cqlType, javaType);
        // double-check that the created codec satisfies the initial request
        if (!codec.accepts(cqlType) || (javaType != null && !codec.accepts(javaType)))
            throw logErrorAndThrow(cqlType, javaType);

        if(logger.isTraceEnabled())
            logger.trace("Codec found: {}", codec);
        // add the codec for future use
        codecs.addIfAbsent(codec);
        return codec;
    }

    private <T> TypeCodec<T> findCodec(DataType cqlType, T value) {
        checkNotNull(value, "Parameter value cannot be null");
        if(logger.isTraceEnabled())
            logger.trace("Looking for codec [{} <-> {}]", cqlType == null ? "ANY" : cqlType, value.getClass());
        for (TypeCodec<?> codec : codecs) {
            if ((cqlType == null || codec.accepts(cqlType)) && codec.accepts(value)) {
                if(logger.isTraceEnabled())
                    logger.trace("Codec found: {}", codec);
                return (TypeCodec<T>)codec;
            }
        }
        // codec not found among the reservoir of known codecs, try to create an ad-hoc codec;
        // this situation should happen mainly when dealing with enums, tuples and UDTs,
        // and collections thereof.
        TypeCodec<T> codec = maybeCreateCodec(cqlType, value);
        if(codec == null)
            throw logErrorAndThrow(cqlType, TypeToken.of(value.getClass()));
        // double-check that the created codec satisfies the initial request
        if((cqlType != null && !codec.accepts(cqlType)) || !codec.accepts(value))
            throw logErrorAndThrow(cqlType, TypeToken.of(value.getClass()));
        if(logger.isTraceEnabled())
            logger.trace("Codec found: {}", codec);
        // add the codec for future use
        codecs.addIfAbsent(codec);
        return codec;
    }

    private <T> TypeCodec<T> maybeCreateCodec(DataType cqlType, TypeToken<T> javaType) {
        if(immutable)
            return null;
        checkNotNull(cqlType);

        if ((cqlType.getName() == VARCHAR || cqlType.getName() == TEXT) && javaType != null && Enum.class.isAssignableFrom(javaType.getRawType())) {
            return new EnumStringCodec(javaType.getRawType());
        }

        if (cqlType.getName() == LIST && (javaType == null || List.class.isAssignableFrom(javaType.getRawType()))) {
            TypeToken<?> elementType = null;
            if (javaType != null && javaType.getType() instanceof ParameterizedType) {
                Type[] typeArguments = ((ParameterizedType)javaType.getType()).getActualTypeArguments();
                elementType = TypeToken.of(typeArguments[0]);
            }
            TypeCodec<?> eltCodec = findCodec(cqlType.getTypeArguments().get(0), elementType);
            return new ListCodec(eltCodec);
        }

        if (cqlType.getName() == SET && (javaType == null || Set.class.isAssignableFrom(javaType.getRawType()))) {
            TypeToken<?> elementType = null;
            if (javaType != null && javaType.getType() instanceof ParameterizedType) {
                Type[] typeArguments = ((ParameterizedType)javaType.getType()).getActualTypeArguments();
                elementType = TypeToken.of(typeArguments[0]);
            }
            TypeCodec<?> eltCodec = findCodec(cqlType.getTypeArguments().get(0), elementType);
            return new SetCodec(eltCodec);
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
            return new MapCodec(keyCodec, valueCodec);
        }

        if (cqlType instanceof TupleType && (javaType == null || TupleValue.class.isAssignableFrom(javaType.getRawType()))) {
            return (TypeCodec<T>)new TupleCodec((TupleType)cqlType);
        }

        if (cqlType instanceof UserType && (javaType == null || UDTValue.class.isAssignableFrom(javaType.getRawType()))) {
            return (TypeCodec<T>)new UDTCodec((UserType)cqlType);
        }

        return null;
    }

    private <T> TypeCodec<T> maybeCreateCodec(DataType cqlType, T value) {
        if(immutable)
            return null;
        checkNotNull(value);
        if ((cqlType == null || cqlType.getName() == VARCHAR || cqlType.getName() == TEXT) && value instanceof Enum) {
            return new EnumStringCodec(value.getClass());
        }

        if ((cqlType == null || cqlType.getName() == LIST) && value instanceof List) {
            List list = (List)value;
            if (list.isEmpty()) {
                DataType elementType = (cqlType == null || cqlType.getTypeArguments().isEmpty())
                    ? DataType.blob()
                    : cqlType.getTypeArguments().get(0);
                return new ListCodec(findCodec(elementType, (TypeToken)null));
            } else {
                DataType elementType = (cqlType == null || cqlType.getTypeArguments().isEmpty())
                    ? null
                    : cqlType.getTypeArguments().get(0);
                return new ListCodec(findCodec(elementType, list.iterator().next()));
            }
        }

        if ((cqlType == null || cqlType.getName() == SET) && value instanceof Set) {
            Set set = (Set)value;
            if (set.isEmpty()) {
                DataType elementType = (cqlType == null || cqlType.getTypeArguments().isEmpty())
                    ? DataType.blob()
                    : cqlType.getTypeArguments().get(0);
                return new SetCodec(findCodec(elementType, (TypeToken)null));
            } else {
                DataType elementType = (cqlType == null || cqlType.getTypeArguments().isEmpty())
                    ? null
                    : cqlType.getTypeArguments().get(0);
                return new SetCodec(findCodec(elementType, set.iterator().next()));
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
                return new MapCodec(
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
                return new MapCodec(
                    findCodec(keyType, entry.getKey()),
                    findCodec(valueType, entry.getValue()));
            }
        }

        if ((cqlType == null || cqlType.getName() == DataType.Name.TUPLE) && value instanceof TupleValue) {
            return (TypeCodec<T>)new TupleCodec(cqlType == null ? ((TupleValue)value).getType() : (TupleType) cqlType);
        }

        if ((cqlType == null || cqlType.getName() == DataType.Name.UDT) && value instanceof UDTValue) {
            return (TypeCodec<T>)new UDTCodec(cqlType == null ? ((UDTValue)value).getType() : (UserType) cqlType);
        }

        return null;
    }

    private static CodecNotFoundException logErrorAndThrow(DataType cqlType, TypeToken<?> javaType) {
        String msg = String.format("Codec not found for requested operation: [%s <-> %s]",
            cqlType == null ? "ANY" : cqlType,
            javaType == null ? "ANY" : javaType);
        logger.error(msg);
        return new CodecNotFoundException(msg, cqlType, javaType);
    }

}
