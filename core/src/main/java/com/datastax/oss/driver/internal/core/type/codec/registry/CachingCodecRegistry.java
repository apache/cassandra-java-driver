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

import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.CustomType;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.protocol.internal.util.IntMap;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A codec registry that handles built-in type mappings, can be extended with a list of
 * user-provided codecs, generates more complex codecs from those basic codecs, and caches generated
 * codecs for reuse.
 *
 * <p>The primitive mappings always take precedence over any user codec. The list of user codecs can
 * not be modified after construction.
 *
 * <p>This class is abstract in order to be agnostic from the cache implementation. Subclasses must
 * implement {@link #getCachedCodec(DataType, GenericType)}.
 */
public abstract class CachingCodecRegistry implements CodecRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(CachingCodecRegistry.class);

  // Implementation notes:
  // - built-in primitive codecs are served directly, without hitting the cache
  // - same for user codecs (we assume the cardinality will always be low, so a sequential array
  //   traversal is cheap).

  protected final String logPrefix;
  private final TypeCodec<?>[] userCodecs;

  protected CachingCodecRegistry(String logPrefix, TypeCodec<?>... userCodecs) {
    this.logPrefix = logPrefix;
    this.userCodecs = userCodecs;
  }

  /**
   * Gets a complex codec from the cache.
   *
   * <p>If the codec does not exist in the cache, this method must generate it with {@link
   * #createCodec(DataType, GenericType)} (and most likely put it in the cache too for future
   * calls).
   */
  protected abstract TypeCodec<?> getCachedCodec(DataType cqlType, GenericType<?> javaType);

  @Override
  public <T> TypeCodec<T> codecFor(DataType cqlType, GenericType<T> javaType) {
    LOG.trace("[{}] Looking up codec for {} <-> {}", logPrefix, cqlType, javaType);
    TypeCodec<?> primitiveCodec = PRIMITIVE_CODECS_BY_CODE.get(cqlType.getProtocolCode());
    if (primitiveCodec != null && primitiveCodec.accepts(javaType)) {
      LOG.trace("[{}] Found matching primitive codec {}", logPrefix, primitiveCodec);
      return safeCast(primitiveCodec);
    }
    for (TypeCodec<?> userCodec : userCodecs) {
      if (userCodec.accepts(cqlType) && userCodec.accepts(javaType)) {
        LOG.trace("[{}] Found matching user codec {}", logPrefix, userCodec);
        return safeCast(userCodec);
      }
    }
    return safeCast(getCachedCodec(cqlType, javaType));
  }

  @Override
  public <T> TypeCodec<T> codecFor(DataType cqlType, Class<T> javaType) {
    LOG.trace("[{}] Looking up codec for {} <-> {}", logPrefix, cqlType, javaType);
    TypeCodec<?> primitiveCodec = PRIMITIVE_CODECS_BY_CODE.get(cqlType.getProtocolCode());
    if (primitiveCodec != null && primitiveCodec.getJavaType().__getToken().getType() == javaType) {
      LOG.trace("[{}] Found matching primitive codec {}", logPrefix, primitiveCodec);
      return safeCast(primitiveCodec);
    }
    for (TypeCodec<?> userCodec : userCodecs) {
      if (userCodec.accepts(cqlType) && userCodec.accepts(javaType)) {
        LOG.trace("[{}] Found matching user codec {}", logPrefix, userCodec);
        return safeCast(userCodec);
      }
    }
    return safeCast(getCachedCodec(cqlType, GenericType.of(javaType)));
  }

  @Override
  public <T> TypeCodec<T> codecFor(DataType cqlType) {
    LOG.trace("[{}] Looking up codec for CQL type {}", logPrefix, cqlType);
    TypeCodec<?> primitiveCodec = PRIMITIVE_CODECS_BY_CODE.get(cqlType.getProtocolCode());
    if (primitiveCodec != null) {
      LOG.trace("[{}] Found matching primitive codec {}", logPrefix, primitiveCodec);
      return safeCast(primitiveCodec);
    }
    for (TypeCodec<?> userCodec : userCodecs) {
      if (userCodec.accepts(cqlType)) {
        LOG.trace("[{}] Found matching user codec {}", logPrefix, userCodec);
        return safeCast(userCodec);
      }
    }
    return safeCast(getCachedCodec(cqlType, null));
  }

  @Override
  public <T> TypeCodec<T> codecFor(T value) {
    Preconditions.checkNotNull(value);
    LOG.trace("[{}] Looking up codec for object {}", logPrefix, value);

    for (TypeCodec<?> primitiveCodec : PRIMITIVE_CODECS) {
      if (primitiveCodec.accepts(value)) {
        LOG.trace("[{}] Found matching primitive codec {}", logPrefix, primitiveCodec);
        return safeCast(primitiveCodec);
      }
    }
    for (TypeCodec<?> userCodec : userCodecs) {
      if (userCodec.accepts(value)) {
        LOG.trace("[{}] Found matching user codec {}", logPrefix, userCodec);
        return safeCast(userCodec);
      }
    }

    if (value instanceof TupleValue) {
      return safeCast(codecFor(((TupleValue) value).getType(), TupleValue.class));
    } else if (value instanceof UdtValue) {
      return safeCast(codecFor(((UdtValue) value).getType(), UdtValue.class));
    }

    GenericType<?> javaType = inspectType(value);
    LOG.trace("[{}] Continuing based on inferred type {}", logPrefix, javaType);
    return safeCast(getCachedCodec(null, javaType));
  }

  // Not exposed publicly, this is only used for the recursion from createCovariantCodec(GenericType)
  private TypeCodec<?> covariantCodecFor(GenericType<?> javaType) {
    LOG.trace("[{}] Looking up codec for Java type {}", logPrefix, javaType);
    for (TypeCodec<?> primitiveCodec : PRIMITIVE_CODECS) {
      if (primitiveCodec.getJavaType().__getToken().isSupertypeOf(javaType.__getToken())) {
        LOG.trace("[{}] Found matching primitive codec {}", logPrefix, primitiveCodec);
        return safeCast(primitiveCodec);
      }
    }
    for (TypeCodec<?> userCodec : userCodecs) {
      if (userCodec.getJavaType().__getToken().isSupertypeOf(javaType.__getToken())) {
        LOG.trace("[{}] Found matching user codec {}", logPrefix, userCodec);
        return safeCast(userCodec);
      }
    }
    return safeCast(getCachedCodec(null, javaType));
  }

  private GenericType<?> inspectType(Object value) {
    if (value instanceof List) {
      List<?> list = (List) value;
      if (list.isEmpty()) {
        // The empty list is always encoded the same way, so any element type will do
        return GenericType.listOf(Boolean.class);
      } else {
        GenericType<?> elementType = inspectType(list.get(0));
        return GenericType.listOf(elementType);
      }
    } else if (value instanceof Set) {
      Set<?> set = (Set) value;
      if (set.isEmpty()) {
        return GenericType.setOf(Boolean.class);
      } else {
        GenericType<?> elementType = inspectType(set.iterator().next());
        return GenericType.setOf(elementType);
      }
    } else if (value instanceof Map) {
      Map<?, ?> map = (Map) value;
      if (map.isEmpty()) {
        return GenericType.mapOf(Boolean.class, Boolean.class);
      } else {
        Map.Entry<?, ?> entry = map.entrySet().iterator().next();
        GenericType<?> keyType = inspectType(entry.getKey());
        GenericType<?> valueType = inspectType(entry.getValue());
        return GenericType.mapOf(keyType, valueType);
      }
    } else {
      // There's not much more we can do
      return GenericType.of(value.getClass());
    }
  }

  // Try to create a codec when we haven't found it in the cache
  protected TypeCodec<?> createCodec(DataType cqlType, GenericType<?> javaType) {
    LOG.trace("[{}] Cache miss, creating codec", logPrefix);
    // Either type can be null, but not both.
    if (javaType == null) {
      assert cqlType != null;
      return createCodec(cqlType);
    } else if (cqlType == null) {
      return createCovariantCodec(javaType);
    }
    TypeToken<?> token = javaType.__getToken();
    if (cqlType instanceof ListType && List.class.isAssignableFrom(token.getRawType())) {
      DataType elementCqlType = ((ListType) cqlType).getElementType();
      TypeCodec<Object> elementCodec;
      if (token.getType() instanceof ParameterizedType) {
        Type[] typeArguments = ((ParameterizedType) token.getType()).getActualTypeArguments();
        GenericType<?> elementJavaType = GenericType.of(typeArguments[0]);
        elementCodec = safeCast(codecFor(elementCqlType, elementJavaType));
      } else {
        elementCodec = codecFor(elementCqlType);
      }
      return TypeCodecs.listOf(elementCodec);
    } else if (cqlType instanceof SetType && Set.class.isAssignableFrom(token.getRawType())) {
      DataType elementCqlType = ((SetType) cqlType).getElementType();
      TypeCodec<Object> elementCodec;
      if (token.getType() instanceof ParameterizedType) {
        Type[] typeArguments = ((ParameterizedType) token.getType()).getActualTypeArguments();
        GenericType<?> elementJavaType = GenericType.of(typeArguments[0]);
        elementCodec = safeCast(codecFor(elementCqlType, elementJavaType));
      } else {
        elementCodec = codecFor(elementCqlType);
      }
      return TypeCodecs.setOf(elementCodec);
    } else if (cqlType instanceof MapType && Map.class.isAssignableFrom(token.getRawType())) {
      DataType keyCqlType = ((MapType) cqlType).getKeyType();
      DataType valueCqlType = ((MapType) cqlType).getValueType();
      TypeCodec<Object> keyCodec;
      TypeCodec<Object> valueCodec;
      if (token.getType() instanceof ParameterizedType) {
        Type[] typeArguments = ((ParameterizedType) token.getType()).getActualTypeArguments();
        GenericType<?> keyJavaType = GenericType.of(typeArguments[0]);
        GenericType<?> valueJavaType = GenericType.of(typeArguments[1]);
        keyCodec = safeCast(codecFor(keyCqlType, keyJavaType));
        valueCodec = safeCast(codecFor(valueCqlType, valueJavaType));
      } else {
        keyCodec = codecFor(keyCqlType);
        valueCodec = codecFor(valueCqlType);
      }
      return TypeCodecs.mapOf(keyCodec, valueCodec);
    } else if (cqlType instanceof TupleType
        && TupleValue.class.isAssignableFrom(token.getRawType())) {
      return TypeCodecs.tupleOf((TupleType) cqlType);
    } else if (cqlType instanceof UserDefinedType
        && UdtValue.class.isAssignableFrom(token.getRawType())) {
      return TypeCodecs.udtOf((UserDefinedType) cqlType);
    } else if (cqlType instanceof CustomType
        && ByteBuffer.class.isAssignableFrom(token.getRawType())) {
      return TypeCodecs.custom(cqlType);
    }
    throw new CodecNotFoundException(cqlType, javaType);
  }

  // Try to create a codec when we haven't found it in the cache.
  // Variant where the CQL type is unknown. Note that this is only used for lookups by Java value,
  // and therefore covariance is allowed.
  private TypeCodec<?> createCovariantCodec(GenericType<?> javaType) {
    TypeToken<?> token = javaType.__getToken();
    if (List.class.isAssignableFrom(token.getRawType())
        && token.getType() instanceof ParameterizedType) {
      Type[] typeArguments = ((ParameterizedType) token.getType()).getActualTypeArguments();
      GenericType<?> elementType = GenericType.of(typeArguments[0]);
      TypeCodec<?> elementCodec = covariantCodecFor(elementType);
      return TypeCodecs.listOf(elementCodec);
    } else if (Set.class.isAssignableFrom(token.getRawType())
        && token.getType() instanceof ParameterizedType) {
      Type[] typeArguments = ((ParameterizedType) token.getType()).getActualTypeArguments();
      GenericType<?> elementType = GenericType.of(typeArguments[0]);
      TypeCodec<?> elementCodec = covariantCodecFor(elementType);
      return TypeCodecs.setOf(elementCodec);
    } else if (Map.class.isAssignableFrom(token.getRawType())
        && token.getType() instanceof ParameterizedType) {
      Type[] typeArguments = ((ParameterizedType) token.getType()).getActualTypeArguments();
      GenericType<?> keyType = GenericType.of(typeArguments[0]);
      GenericType<?> valueType = GenericType.of(typeArguments[1]);
      TypeCodec<?> keyCodec = covariantCodecFor(keyType);
      TypeCodec<?> valueCodec = covariantCodecFor(valueType);
      return TypeCodecs.mapOf(keyCodec, valueCodec);
    }
    throw new CodecNotFoundException(null, javaType);
  }

  // Try to create a codec when we haven't found it in the cache.
  // Variant where the Java type is unknown.
  private TypeCodec<?> createCodec(DataType cqlType) {
    if (cqlType instanceof ListType) {
      DataType elementType = ((ListType) cqlType).getElementType();
      TypeCodec<Object> elementCodec = codecFor(elementType);
      return TypeCodecs.listOf(elementCodec);
    } else if (cqlType instanceof SetType) {
      DataType elementType = ((SetType) cqlType).getElementType();
      TypeCodec<Object> elementCodec = codecFor(elementType);
      return TypeCodecs.setOf(elementCodec);
    } else if (cqlType instanceof MapType) {
      DataType keyType = ((MapType) cqlType).getKeyType();
      DataType valueType = ((MapType) cqlType).getValueType();
      TypeCodec<Object> keyCodec = codecFor(keyType);
      TypeCodec<Object> valueCodec = codecFor(valueType);
      return TypeCodecs.mapOf(keyCodec, valueCodec);
    } else if (cqlType instanceof TupleType) {
      return TypeCodecs.tupleOf((TupleType) cqlType);
    } else if (cqlType instanceof UserDefinedType) {
      return TypeCodecs.udtOf((UserDefinedType) cqlType);
    } else if (cqlType instanceof CustomType) {
      return TypeCodecs.custom(cqlType);
    }
    throw new CodecNotFoundException(cqlType, null);
  }

  // roughly sorted by popularity
  private static final TypeCodec<?>[] PRIMITIVE_CODECS =
      new TypeCodec<?>[] {
        // Must be declared before AsciiCodec so it gets chosen when CQL type not available
        TypeCodecs.TEXT,
        // Must be declared before TimeUUIDCodec so it gets chosen when CQL type not available
        TypeCodecs.UUID,
        TypeCodecs.TIMEUUID,
        TypeCodecs.TIMESTAMP,
        TypeCodecs.INT,
        TypeCodecs.BIGINT,
        TypeCodecs.BLOB,
        TypeCodecs.DOUBLE,
        TypeCodecs.FLOAT,
        TypeCodecs.DECIMAL,
        TypeCodecs.VARINT,
        TypeCodecs.INET,
        TypeCodecs.BOOLEAN,
        TypeCodecs.SMALLINT,
        TypeCodecs.TINYINT,
        TypeCodecs.DATE,
        TypeCodecs.TIME,
        TypeCodecs.DURATION,
        TypeCodecs.COUNTER,
        TypeCodecs.ASCII
      };

  private static final IntMap<TypeCodec> PRIMITIVE_CODECS_BY_CODE =
      sortByProtocolCode(PRIMITIVE_CODECS);

  private static IntMap<TypeCodec> sortByProtocolCode(TypeCodec<?>[] codecs) {
    IntMap.Builder<TypeCodec> builder = IntMap.builder();
    for (TypeCodec<?> codec : codecs) {
      builder.put(codec.getCqlType().getProtocolCode(), codec);
    }
    return builder.build();
  }

  // We call this after validating the types, so we know the cast will never fail.
  private static <T, U> TypeCodec<T> safeCast(TypeCodec<U> codec) {
    @SuppressWarnings("unchecked")
    TypeCodec<T> result = (TypeCodec<T>) codec;
    return result;
  }
}
