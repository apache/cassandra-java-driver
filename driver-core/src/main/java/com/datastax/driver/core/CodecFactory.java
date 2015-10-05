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
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

import com.datastax.driver.core.TypeCodec.*;

import static com.datastax.driver.core.DataType.Name.*;

/**
 * A factory for {@link TypeCodec} instances.
 * Instances of this interface are used by the {@link CodecRegistry} to create
 * codecs on-the-fly.
 */
public interface CodecFactory {

    /**
     * The default instance.
     */
    CodecFactory DEFAULT_INSTANCE = new DefaultCodecFactory();

    /**
     * Create a new codec for the given CQL type and Java type pair.
     * This method should return {@code null} to indicate that a suitable codec cannot be created.
     *
     * @param cqlType The {@link DataType CQL type} the codec should accept; must not be {@code null}.
     * @param javaType The {@link TypeToken Java type} the codec should accept; can be {@code null}.
     * @param codecRegistry The {@link CodecRegistry} instance to use when creating composite codecs.
     * @return A newly-created codec, or {@code null} if none could be created.
     */
    <T> TypeCodec<T> newCodec(DataType cqlType, TypeToken<T> javaType, CodecRegistry codecRegistry);

    /**
     * Create a new codec for the given CQL type and Java type pair.
     * This method should return {@code null} to indicate that a suitable codec cannot be created.
     *
     * @param cqlType The {@link DataType CQL type} the codec should accept; can be {@code null}.
     * @param value The value the codec should accept; must not be {@code null}.
     * @param codecRegistry The {@link CodecRegistry} instance to use when creating composite codecs.
     * @return A newly-created codec, or {@code null} if none could be created.
     */
    <T> TypeCodec<T> newCodec(DataType cqlType, T value, CodecRegistry codecRegistry);

    /**
     * The default {@link CodecFactory} used by the {@link CodecRegistry}.
     * It is capable of creating the following codecs on-the-fly:
     * <ul>
     *     <li>Codecs for CQL collections ({@code list}, {@code set} and {@code map}) are created with {@link ListCodec},  {@link SetCodec} and  {@link MapCodec} respectively;</li>
     *     <li>Codecs for User-defined types (UDTs) are created with {@link UDTCodec};</li>
     *     <li>Codecs for Tuple types are created with {@link TupleCodec}.</li>
     *     <li>Codecs for Java enums are created with {@link EnumStringCodec} or {@link EnumIntCodec}, depending on the CQL type;</li>
     * </ul>
     */
    class DefaultCodecFactory implements CodecFactory {

        @SuppressWarnings("ALL")
        @Override
        public <T> TypeCodec<T> newCodec(DataType cqlType, TypeToken<T> javaType, CodecRegistry codecRegistry) {

            checkNotNull(cqlType, "Parameter cqlType cannot be null");

            if (cqlType.getName() == LIST && (javaType == null || List.class.isAssignableFrom(javaType.getRawType()))) {
                TypeToken<?> elementType = null;
                if (javaType != null && javaType.getType() instanceof ParameterizedType) {
                    Type[] typeArguments = ((ParameterizedType)javaType.getType()).getActualTypeArguments();
                    elementType = TypeToken.of(typeArguments[0]);
                }
                TypeCodec<?> eltCodec = codecRegistry.codecFor(cqlType.getTypeArguments().get(0), elementType);
                return (TypeCodec<T>)newListCodec(eltCodec);
            }

            if (cqlType.getName() == SET && (javaType == null || Set.class.isAssignableFrom(javaType.getRawType()))) {
                TypeToken<?> elementType = null;
                if (javaType != null && javaType.getType() instanceof ParameterizedType) {
                    Type[] typeArguments = ((ParameterizedType)javaType.getType()).getActualTypeArguments();
                    elementType = TypeToken.of(typeArguments[0]);
                }
                TypeCodec<?> eltCodec = codecRegistry.codecFor(cqlType.getTypeArguments().get(0), elementType);
                return (TypeCodec<T>)newSetCodec(eltCodec);
            }

            if (cqlType.getName() == MAP && (javaType == null || Map.class.isAssignableFrom(javaType.getRawType()))) {
                TypeToken<?> keyType = null;
                TypeToken<?> valueType = null;
                if (javaType != null && javaType.getType() instanceof ParameterizedType) {
                    Type[] typeArguments = ((ParameterizedType)javaType.getType()).getActualTypeArguments();
                    keyType = TypeToken.of(typeArguments[0]);
                    valueType = TypeToken.of(typeArguments[1]);
                }
                TypeCodec<?> keyCodec = codecRegistry.codecFor(cqlType.getTypeArguments().get(0), keyType);
                TypeCodec<?> valueCodec = codecRegistry.codecFor(cqlType.getTypeArguments().get(1), valueType);
                return (TypeCodec<T>)newMapCodec(keyCodec, valueCodec);
            }

            if (cqlType instanceof TupleType && (javaType == null || TupleValue.class.isAssignableFrom(javaType.getRawType()))) {
                return (TypeCodec<T>)new TupleCodec((TupleType)cqlType);
            }

            if (cqlType instanceof UserType && (javaType == null || UDTValue.class.isAssignableFrom(javaType.getRawType()))) {
                return (TypeCodec<T>)new UDTCodec((UserType)cqlType);
            }

            if ((cqlType.getName() == VARCHAR || cqlType.getName() == TEXT) && javaType != null && Enum.class.isAssignableFrom(javaType.getRawType())) {
                return new EnumStringCodec(javaType.getRawType());
            }

            if (cqlType != null && cqlType.getName() == INT && Enum.class.isAssignableFrom(javaType.getRawType())) {
                return new EnumIntCodec(javaType.getRawType());
            }

            return null;

        }

        @SuppressWarnings("ALL")
        @Override
        public <T> TypeCodec<T> newCodec(DataType cqlType, T value, CodecRegistry codecRegistry) {

            checkNotNull(value, "Parameter value cannot be null");

            if ((cqlType == null || cqlType.getName() == LIST) && value instanceof List) {
                List list = (List)value;
                if (list.isEmpty()) {
                    DataType elementType = (cqlType == null || cqlType.getTypeArguments().isEmpty())
                        ? DataType.blob()
                        : cqlType.getTypeArguments().get(0);
                    return newListCodec(codecRegistry.codecFor(elementType, (TypeToken)null));
                } else {
                    DataType elementType = (cqlType == null || cqlType.getTypeArguments().isEmpty())
                        ? null
                        : cqlType.getTypeArguments().get(0);
                    return (TypeCodec<T>)newListCodec(codecRegistry.codecFor(elementType, list.iterator().next()));
                }
            }

            if ((cqlType == null || cqlType.getName() == SET) && value instanceof Set) {
                Set set = (Set)value;
                if (set.isEmpty()) {
                    DataType elementType = (cqlType == null || cqlType.getTypeArguments().isEmpty())
                        ? DataType.blob()
                        : cqlType.getTypeArguments().get(0);
                    return newSetCodec(codecRegistry.codecFor(elementType, (TypeToken)null));
                } else {
                    DataType elementType = (cqlType == null || cqlType.getTypeArguments().isEmpty())
                        ? null
                        : cqlType.getTypeArguments().get(0);
                    return (TypeCodec<T>)newSetCodec(codecRegistry.codecFor(elementType, set.iterator().next()));
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
                    return newMapCodec(
                        codecRegistry.codecFor(keyType, (TypeToken)null),
                        codecRegistry.codecFor(valueType, (TypeToken)null));
                } else {
                    DataType keyType = (cqlType == null || cqlType.getTypeArguments().size() < 1)
                        ? null
                        : cqlType.getTypeArguments().get(0);
                    DataType valueType = (cqlType == null || cqlType.getTypeArguments().size() < 2)
                        ? null
                        : cqlType.getTypeArguments().get(1);
                    Map.Entry entry = (Map.Entry)map.entrySet().iterator().next();
                    return (TypeCodec<T>)newMapCodec(
                        codecRegistry.codecFor(keyType, entry.getKey()),
                        codecRegistry.codecFor(valueType, entry.getValue()));
                }
            }

            if ((cqlType == null || cqlType.getName() == DataType.Name.TUPLE) && value instanceof TupleValue) {
                return (TypeCodec<T>)new TupleCodec(cqlType == null ? ((TupleValue)value).getType() : (TupleType)cqlType);
            }

            if ((cqlType == null || cqlType.getName() == DataType.Name.UDT) && value instanceof UDTValue) {
                return (TypeCodec<T>)new UDTCodec(cqlType == null ? ((UDTValue)value).getType() : (UserType)cqlType);
            }

            if ((cqlType == null || cqlType.getName() == VARCHAR || cqlType.getName() == TEXT) && value instanceof Enum) {
                return new EnumStringCodec(value.getClass());
            }

            if (cqlType != null && cqlType.getName() == INT && value instanceof Enum) {
                return new EnumIntCodec(value.getClass());
            }

            return null;

        }

        /**
         * Hook for subclasses wishing to customize codecs for CQL {@code list} types.
         * @param eltCodec The element codec.
         * @return A suitable codec.
         */
        protected <T> TypeCodec<List<T>> newListCodec(TypeCodec<T> eltCodec) {
            return new ListCodec<T>(eltCodec);
        }

        /**
         * Hook for subclasses wishing to customize codecs for CQL {@code set} types.
         * @param eltCodec The element codec.
         * @return A suitable codec.
         */
        protected <T> TypeCodec<Set<T>> newSetCodec(TypeCodec<T> eltCodec) {
            return new SetCodec<T>(eltCodec);
        }

        /**
         * Hook for subclasses wishing to customize codecs for CQL {@code map} types.
         * @param keyCodec The map key codec.
         * @param valueCodec The map value codec.
         * @return A suitable codec.
         */
        protected <K, V> TypeCodec<Map<K, V>> newMapCodec(TypeCodec<K> keyCodec, TypeCodec<V> valueCodec) {
            return new MapCodec<K, V>(keyCodec, valueCodec);
        }

    }
}
