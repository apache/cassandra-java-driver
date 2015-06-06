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

import java.util.Arrays;
import java.util.Collections;

import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

import com.datastax.driver.core.TypeCodec.*;
import com.datastax.driver.core.exceptions.CodecNotFoundException;

/**
 * A registry for {@link TypeCodec}s.
 * <p>
 * {@link CodecRegistry} instances can be created via the {@link #builder()} method:
 * <pre>
 * CodecRegistry registry = CodecRegistry.builder().withCodecs(codec1, codec2).build();
 * </pre>
 * Note that the order in which codecs are added to the registry matters;
 * when looking for a matching codec, the registry will consider all known codecs in
 * the order they have been provided and will return the first matching candidate,
 * even if another codec would be a better match.
 * <p>
 * To build a {@link CodecRegistry} instance with all the default codecs used
 * by the Java driver, simply use the following:
 * <pre>
 * CodecRegistry registry = CodecRegistry.builder().withDefaultCodecs().build();
 * </pre>
 * Note that the default set of codecs has no support for
 * <a href="https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/db/marshal/AbstractType.java">Cassandra custom types</a>;
 * to be able to deserialize values of such types, you need to manually register an appropriate codec:
 * <pre>
 * CodecRegistry registry = CodecRegistry.builder().withCustomType(myCustomTypeCodec).build();
 * </pre>
 * {@link CodecRegistry} instances must then be associated with a {@link Cluster} instance:
 * <pre>
 * Cluster cluster = new Cluster.builder().withCodecRegistry(myCodecRegistry).build();
 * </pre>
 * The default {@link CodecRegistry} instance is {@link CodecRegistry#DEFAULT_INSTANCE}.
 * To retrieve the {@link CodecRegistry} instance associated with a Cluster, do the
 * following:
 * <pre>
 * CodecRegistry registry = cluster.getConfiguration().getCodecRegistry();
 * </pre>
 * {@link CodecRegistry} instances are immutable and can be safely shared accross threads.
 *
 */
public class CodecRegistry {

    private static final Logger logger = LoggerFactory.getLogger(CodecRegistry.class);

    private static final ImmutableSet<TypeCodec<?>> PRIMITIVE_CODECS = ImmutableSet.of(
        BlobCodec.instance,
        BooleanCodec.instance,
        IntCodec.instance,
        BigintCodec.instance,
        CounterCodec.instance,
        DoubleCodec.instance,
        FloatCodec.instance,
        VarintCodec.instance,
        DecimalCodec.instance,
        VarcharCodec.instance,
        AsciiCodec.instance,
        TimestampCodec.instance,
        UUIDCodec.instance,
        TimeUUIDCodec.instance,
        InetCodec.instance
    );

    /**
     * The default set of codecs used by the Java driver.
     * They contain codecs to handle native types, and collections
     * of all native types (lists, sets and maps).
     *
     * Note that the default set of codecs has no support for
     * <a href="https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/db/marshal/AbstractType.java">Cassandra custom types</a>;
     * to be able to deserialize values of such types, you need to manually register an appropriate codec.
     */
    private static final ImmutableList<TypeCodec<?>> DEFAULT_CODECS;

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
     * The default {@link CodecRegistry} instance.
     */
    public static final CodecRegistry DEFAULT_INSTANCE = new CodecRegistry(DEFAULT_CODECS, ImmutableMap.<OverrideKey, TypeCodec<?>>of());

    /**
     * A builder to build {@link CodecRegistry} instances.
     */
    public static class Builder {

        private ImmutableList.Builder<TypeCodec<?>> builder = ImmutableList.builder();

        private ImmutableMap.Builder<OverrideKey, TypeCodec<?>> overrides = ImmutableMap.builder();

        /**
         * Add the {@link #DEFAULT_CODECS default set of codecs} to the registry.
         *
         * @return this Builder (for method chaining)
         */
        public Builder withDefaultCodecs() {
            builder.addAll(DEFAULT_CODECS);
            return this;
        }

        /**
         * Add the given codec to the list of known codecs of this registry.
         * Note that the order in which codecs are added to the registry matters;
         * when looking for a matching codec, the registry will consider all known codecs in
         * the order they have been provided and will return the first matching candidate,
         * even if another codec would be a better match.
         *
         * @param codec The codec to add to the registry.
         * @return this Builder (for method chaining).
         */
        public Builder withCodec(TypeCodec<?> codec) {
            return withCodec(codec, false);
        }

        /**
         * Add the given codec to the list of known codecs of this registry.
         * Note that the order in which codecs are added to the registry matters;
         * when looking for a matching codec, the registry will consider all known codecs in
         * the order they have been provided and will return the first matching candidate,
         * even if another codec would be a better match.
         *
         * @param codec The codec to add to the registry.
         * @param includeDerivedCollectionCodecs If {@code true}, register not only the given codec
         * but also collection codecs whose element types are handled by the given codec.
         * @return this Builder (for method chaining).
         */
        public Builder withCodec(TypeCodec<?> codec, boolean includeDerivedCollectionCodecs) {
            return withCodecs(Collections.singleton(codec), includeDerivedCollectionCodecs);
        }

        /**
         * Add the given codecs to the list of known codecs of this registry.
         * Note that the order in which codecs are added to the registry matters;
         * when looking for a matching codec, the registry will consider all known codecs in
         * the order they have been provided and will return the first matching candidate,
         * even if another codec would be a better match.
         *
         * @param codecs The codecs to add to the registry.
         * @return this Builder (for method chaining).
         */
        public Builder withCodecs(TypeCodec<?>... codecs) {
            return withCodecs(Arrays.asList(codecs), false);
        }

        /**
         * Add the given codecs to the list of known codecs of this registry.
         * Note that the order in which codecs are added to the registry matters;
         * when looking for a matching codec, the registry will consider all known codecs in
         * the order they have been provided and will return the first matching candidate,
         * even if another codec would be a better match.
         *
         * @param codecs The codecs to add to the registry.
         * @return this Builder (for method chaining).
         */
        public Builder withCodecs(Iterable<? extends TypeCodec<?>> codecs) {
            return withCodecs(codecs, false);
        }

        /**
         * Add the given codecs to the list of known codecs of this registry.
         * Note that the order in which codecs are added to the registry matters;
         * when looking for a matching codec, the registry will consider all known codecs in
         * the order they have been provided and will return the first matching candidate,
         * even if another codec would be a better match.
         *
         * @param codecs The codecs to add to the registry.
         * @param includeDerivedCollectionCodecs If {@code true}, register not only the given codecs
         * but also collection codecs whose element types are handled by the given codecs.
         * @return this Builder (for method chaining).
         */
        public Builder withCodecs(Iterable<? extends TypeCodec<?>> codecs, boolean includeDerivedCollectionCodecs) {
            for (TypeCodec<?> codec : codecs) {
                if(includeDerivedCollectionCodecs) addCodecAndDerivedCollectionCodecs(codec);
                else builder.add(codec);
            }
            return this;
        }

        /**
         * Add the given codec to the registry as an "overriding" codec.
         * <p>
         * This codec will only be used if the value being serialized or deserialized
         * belongs to the specified keyspace, table and column.
         * <p>
         * <strong>IMPORTANT</strong>: Overriding codecs can only be used in conjunction with {@link GettableData} instances,
         * such as{@link BoundStatement}s and {@link Row}s.
         * It <em>cannot</em> be used with other instances of {@link Statement}, such
         * as {@link SimpleStatement} or with the {@link com.datastax.driver.core.querybuilder.QueryBuilder query builder}.
         * <p>
         * <em>For this reason, if you plan to use overriding codecs, be sure to only use {@link PreparedStatement}s.</em>
         *
         * @param keyspace The keyspace name to which the overriding codec applies; must not be {@code null}.
         * @param table The table name to which the overriding codec applies; must not be {@code null}.
         * @param column The column name to which the overriding codec applies; must not be {@code null}.
         * @param codec The overriding codec.
         * @return this Builder (for method chaining).
         */
        public Builder withOverridingCodec(String keyspace, String table, String column, TypeCodec<?> codec) {
            checkNotNull(keyspace, "Parameter keyspace cannot be null");
            checkNotNull(table, "Parameter table cannot be null");
            checkNotNull(column, "Parameter column cannot be null");
            overrides.put(new OverrideKey(keyspace, table, column), codec);
            return this;
        }

        /**
         * Builds the registry.
         * @return the newly-created {@link CodecRegistry}.
         */
        public CodecRegistry build() {
            // reverse to make the last codecs override the first ones
            // if their scope overlap
            return new CodecRegistry(builder.build(), overrides.build());
        }

        private <T> void addCodecAndDerivedCollectionCodecs(TypeCodec<T> customCodec) {
            builder.add(customCodec);
            builder.add(new ListCodec<T>(customCodec));
            builder.add(new SetCodec<T>(customCodec));
            for (TypeCodec<?> primitiveCodec : PRIMITIVE_CODECS) {
                builder.add(new MapCodec(primitiveCodec, customCodec));
                builder.add(new MapCodec(customCodec, primitiveCodec));
            }
        }


    }

    /**
     * Create a new instance of {@link com.datastax.driver.core.CodecRegistry.Builder}
     * @return an instance of {@link com.datastax.driver.core.CodecRegistry.Builder}
     * to build {@link CodecRegistry} instances.
     */
    public static CodecRegistry.Builder builder() {
        return new Builder();
    }

    private static final class OverrideKey {

        private final String keysapce;

        private final String table;

        private final String column;

        public OverrideKey(String keysapce, String table, String column) {
            this.keysapce = keysapce;
            this.table = table;
            this.column = column;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            OverrideKey override = (OverrideKey)o;
            return Objects.equal(keysapce, override.keysapce) &&
                Objects.equal(table, override.table) &&
                Objects.equal(column, override.column);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(keysapce, table, column);
        }
    }

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

    private final ImmutableList<TypeCodec<?>> codecs;

    private final ImmutableMap<OverrideKey, TypeCodec<?>> overrides;

    private final LoadingCache<CacheKey, TypeCodec<?>> cache;

    private CodecRegistry(ImmutableList<TypeCodec<?>> codecs, ImmutableMap<OverrideKey, TypeCodec<?>> overrides) {
        this.codecs = codecs;
        this.overrides = overrides;
        this.cache = CacheBuilder.newBuilder()
            .initialCapacity(400)
            .build(
                new CacheLoader<CacheKey, TypeCodec<?>>() {
                    public TypeCodec<?> load(CacheKey cacheKey) {
                        return findCodec(cacheKey.cqlType, cacheKey.javaType);
                    }
                });
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
     * reserved for situations where the runtime object to be serialized is not available or unknown.
     * In the Java driver, this happens mainly when serializing a value in a {@link SimpleStatement}
     * or in the {@link com.datastax.driver.core.querybuilder.QueryBuilder}, where no CQL type information
     * is available.
     * <p>
     * Regular users should avoid using this method.
     * <p>
     * Codecs returned by this method are NOT cached.
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
     * This method handles parameterized types thanks to Guava's {@link TypeToken} API, and should
     * therefore be preferred over {@link #codecFor(DataType, Class)}.
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
     * Codecs returned by this method are NOT cached.
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

    // Methods accepting codec overrides

    /**
     * Return an overriding {@link TypeCodec codec} that accepts the given {@link DataType CQL type}
     * and applies only to the given keyspace, table and column.
     * <p>
     * If such an overriding codec cannot be found, the registry attempts to find
     * a globally-registered one and returns it instead.
     * <p>
     * This method would return the first matching codec, regardless of its accepted Java type.
     * For this reason, this method might not return the most accurate codec and should be
     * reserved for situations where the runtime object to be serialized is not available or unknown.
     * In the Java driver, this happens mainly when deserializing a value using the
     * {@link AbstractGettableData#getObject(int)} method.
     * <p>
     * Codecs returned by this method are cached.
     *
     * @param cqlType The {@link DataType CQL type} the codec should accept; must not be {@code null}.
     * @param keyspace The keyspace name to which the overriding codec applies;
     * if {@code null}, no overriding codec will be located and the globally-registered codec will be returned instead.
     * @param table The table name to which the overriding codec applies;
     * if {@code null}, no overriding codec will be located and the globally-registered codec will be returned instead.
     * @param column The column name to which the overriding codec applies;
     * if {@code null}, no overriding codec will be located and the globally-registered codec will be returned instead.
     * @return An overriding codec for the given keyspace, table and column, if any; otherwise, a globally-registered suitable codec.
     * @throws CodecNotFoundException if an overriding codec is found but does not accept the given {@link DataType CQL type};
     * or if no suitable codec could be found at all.
     */
    public <T> TypeCodec<T> codecFor(DataType cqlType, String keyspace, String table, String column) throws CodecNotFoundException {
        checkNotNull(cqlType, "Parameter cqlType cannot be null");
        TypeCodec<T> codec = findOverridingCodec(cqlType, null, keyspace, table, column);
        if (codec != null)
            return codec;
        return codecFor(cqlType);
    }

    /**
     * Return an overriding {@link TypeCodec codec} that accepts the given {@link DataType CQL type}
     * and the given Java class, and that applies only to the given keyspace, table and column.
     * <p>
     * If such an overriding codec cannot be found, the registry attempts to find
     * a globally-registered one and returns it instead.
     * <p>
     * This method can only handle raw (non-parameterized) Java types.
     * For parameterized types, use {@link #codecFor(DataType, TypeToken, String, String, String)} instead.
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
     * @param keyspace The keyspace name to which the overriding codec applies;
     * if {@code null}, no overriding codec will be located and the globally-registered codec will be returned instead.
     * @param table The table name to which the overriding codec applies;
     * if {@code null}, no overriding codec will be located and the globally-registered codec will be returned instead.
     * @param column The column name to which the overriding codec applies;
     * if {@code null}, no overriding codec will be located and the globally-registered codec will be returned instead.
     * @return An overriding codec for the given keyspace, table and column, if any; otherwise, a globally-registered suitable codec.
     * @throws CodecNotFoundException if an overriding codec is found but does not accept the given {@link DataType CQL type}
     * and the given Java class; or if no suitable codec could be found at all.
     */
    public <T> TypeCodec<T> codecFor(DataType cqlType, Class<T> javaType, String keyspace, String table, String column) throws CodecNotFoundException {
        return codecFor(cqlType, TypeToken.of(javaType), keyspace, table, column);
    }

    /**
     * Return an overriding {@link TypeCodec codec} that accepts the given {@link DataType CQL type}
     * and the given Java type, and that applies only to the given keyspace, table and column.
     * <p>
     * If such an overriding codec cannot be found, the registry attempts to find
     * a globally-registered one and returns it instead.
     * <p>
     * This method handles parameterized types thanks to Guava's {@link TypeToken} API, and should
     * therefore be preferred over {@link #codecFor(DataType, Class, String, String, String)}.
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
     * @param keyspace The keyspace name to which the overriding codec applies;
     * if {@code null}, no overriding codec will be located and the globally-registered codec will be returned instead.
     * @param table The table name to which the overriding codec applies;
     * if {@code null}, no overriding codec will be located and the globally-registered codec will be returned instead.
     * @param column The column name to which the overriding codec applies;
     * if {@code null}, no overriding codec will be located and the globally-registered codec will be returned instead.
     * @return An overriding codec for the given keyspace, table and column, if any; otherwise, a globally-registered suitable codec.
     * @throws CodecNotFoundException if an overriding codec is found but does not accept the given {@link DataType CQL type}
     * and the given Java type; or if no suitable codec could be found at all.
     */
    public <T> TypeCodec<T> codecFor(DataType cqlType, TypeToken<T> javaType, String keyspace, String table, String column) throws CodecNotFoundException {
        checkNotNull(cqlType, "Parameter cqlType cannot be null");
        checkNotNull(javaType, "Parameter javaType cannot be null");
        TypeCodec<T> codec = findOverridingCodec(cqlType, javaType, keyspace, table, column);
        if (codec != null)
            return codec;
        return codecFor(cqlType, javaType);
    }

    /**
     * Return an overriding {@link TypeCodec codec} that accepts the given {@link DataType CQL type}
     * and the given value, and that applies only to the given keyspace, table and column.
     * <p>
     * If such an overriding codec cannot be found, the registry attempts to find
     * a globally-registered one and returns it instead.
     * <p>
     * This method takes an actual Java object and tries to locate a suitable codec for it.
     * For this reason, codecs must perform a {@link TypeCodec#accepts(Object) "manual" inspection}
     * of the object to determine if they can accept it or not, which, depending on the implementations,
     * can be an expensive operation; besides, the resulting codec cannot be cached.
     * Therefore there might be a performance penalty when using this method.
     * <p>
     * Codecs returned by this method are NOT cached.
     *
     * @param cqlType The {@link DataType CQL type} the codec should accept; must not be {@code null}.
     * @param value The value the codec should accept; must not be {@code null}.
     * @param keyspace The keyspace name to which the overriding codec applies;
     * if {@code null}, no overriding codec will be located and the globally-registered codec will be returned instead.
     * @param table The table name to which the overriding codec applies;
     * if {@code null}, no overriding codec will be located and the globally-registered codec will be returned instead.
     * @param column The column name to which the overriding codec applies;
     * if {@code null}, no overriding codec will be located and the globally-registered codec will be returned instead.
     * @return An overriding codec for the given keyspace, table and column, if any, otherwise, a globally-registered suitable codec.
     * @throws CodecNotFoundException if an overriding codec is found but does not accept the given value;
     * or if no suitable codec could be found at all.
     */
    public <T> TypeCodec<T> codecFor(DataType cqlType, T value, String keyspace, String table, String column) {
        checkNotNull(cqlType, "Parameter cqlType cannot be null");
        checkNotNull(value, "Parameter value cannot be null");
        TypeCodec<T> codec = findOverridingCodec(cqlType, value, keyspace, table, column);
        if (codec != null)
            return codec;
        return codecFor(cqlType, value);
    }

    @SuppressWarnings("unchecked")
    private <T> TypeCodec<T> lookupCache(DataType cqlType, TypeToken<T> javaType) {
        logger.debug("Looking up overriding codec for {} <-> {}", cqlType, javaType);
        CacheKey cacheKey = new CacheKey(javaType, cqlType);
        try {
            return (TypeCodec<T>)cache.getUnchecked(cacheKey);
        } catch (UncheckedExecutionException e) {
            throw (CodecNotFoundException)e.getCause();
        }
    }

    @SuppressWarnings("unchecked")
    private <T> TypeCodec<T> findCodec(DataType cqlType, TypeToken<T> javaType) {
        logger.debug("Looking for codec [{} <-> {}]", cqlType, javaType);
        for (TypeCodec<?> codec : codecs) {
            if ((cqlType == null || codec.accepts(cqlType)) && (javaType == null || codec.accepts(javaType))) {
                logger.debug("Codec found: {}", codec);
                return (TypeCodec<T>)codec;
            }
        }
        return logErrorAndThrow(cqlType, javaType);
    }

    @SuppressWarnings("unchecked")
    private <T> TypeCodec<T> findCodec(DataType cqlType, T value) {
        TypeToken<?> javaType = TypeToken.of(value.getClass());
        logger.debug("Looking for codec [{} <-> {}]", cqlType, javaType);
        for (TypeCodec<?> codec : codecs) {
            if ((cqlType == null || codec.accepts(cqlType)) && codec.accepts(value)) {
                logger.debug("Codec found: {}", codec);
                return (TypeCodec<T>)codec;
            }
        }
        return logErrorAndThrow(cqlType, javaType);
    }

    @SuppressWarnings("unchecked")
    private <T> TypeCodec<T> findOverridingCodec(DataType cqlType, TypeToken<T> javaType, String keyspace, String table, String column) {
        if (keyspace != null && table != null && column != null) {
            logger.debug("Looking for overriding codec for {}.{}.{}", keyspace, table, column);
            OverrideKey overrideKey = new OverrideKey(keyspace, table, column);
            if (overrides.containsKey(overrideKey)) {
                TypeCodec<?> codec = overrides.get(overrideKey);
                if ((cqlType == null || codec.accepts(cqlType)) && (javaType == null || codec.accepts(javaType))) {
                    logger.debug("Overriding codec found for {}.{}.{}: {}", keyspace, table, column, codec);
                    return (TypeCodec<T>)codec;
                } else {
                    return logErrorAndThrow(codec, cqlType, javaType, keyspace, table, column);
                }
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private <T> TypeCodec<T> findOverridingCodec(DataType cqlType, T value, String keyspace, String table, String column) {
        if (keyspace != null && table != null && column != null) {
            logger.debug("Looking for overriding codec for {}.{}.{}", keyspace, table, column);
            OverrideKey overrideKey = new OverrideKey(keyspace, table, column);
            if (overrides.containsKey(overrideKey)) {
                TypeCodec<?> codec = overrides.get(overrideKey);
                if ((cqlType == null || codec.accepts(cqlType)) && codec.accepts(value)) { // value cannot be null
                    logger.debug("Overriding codec found for {}.{}.{}: {}", keyspace, table, column, codec);
                    return (TypeCodec<T>)codec;
                } else {
                    return logErrorAndThrow(codec, cqlType, TypeToken.of((Class<T>)value.getClass()), keyspace, table, column);
                }
            }
        }
        return null;
    }

    private <T> TypeCodec<T> logErrorAndThrow(DataType cqlType, TypeToken<?> javaType) {
        String msg = String.format("Codec not found for required pair: [%s <-> %s]",
            cqlType == null ? "ANY" : cqlType,
            javaType == null ? "ANY" : javaType);
        logger.error(msg);
        throw new CodecNotFoundException(msg, cqlType, javaType);
    }

    private <T> TypeCodec<T> logErrorAndThrow(TypeCodec<?> codec, DataType cqlType, TypeToken<T> javaType, String keyspace, String table, String column) {
        String msg = String.format("Found overriding codec %s for %s.%s.%s but it does not accept required pair: [%s <-> %s]",
            codec,
            keyspace, table, column,
            cqlType == null ? "ANY" : cqlType,
            javaType == null ? "ANY" : javaType);
        logger.error(msg);
        throw new CodecNotFoundException(msg, cqlType, javaType);
    }

}
