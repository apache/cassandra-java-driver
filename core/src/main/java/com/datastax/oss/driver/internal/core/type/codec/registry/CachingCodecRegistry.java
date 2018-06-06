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
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.reflect.TypeToken;
import com.datastax.oss.protocol.internal.util.IntMap;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.jcip.annotations.ThreadSafe;
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
 * implement {@link #getCachedCodec(DataType, GenericType, boolean)}.
 */
@ThreadSafe
public abstract class CachingCodecRegistry implements CodecRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(CachingCodecRegistry.class);

  // Implementation notes:
  // - built-in primitive codecs are served directly, without hitting the cache
  // - same for user codecs (we assume the cardinality will always be low, so a sequential array
  //   traversal is cheap).

  protected final String logPrefix;
  private final TypeCodec<?>[] primitiveCodecs;
  private final TypeCodec<?>[] userCodecs;
  private final IntMap<TypeCodec> primitiveCodecsByCode;

  protected CachingCodecRegistry(
      String logPrefix, TypeCodec<?>[] primitiveCodecs, TypeCodec<?>[] userCodecs) {
    this.logPrefix = logPrefix;
    this.primitiveCodecs = primitiveCodecs;
    this.userCodecs = userCodecs;
    this.primitiveCodecsByCode = sortByProtocolCode(primitiveCodecs);
  }

  /**
   * Gets a complex codec from the cache.
   *
   * <p>If the codec does not exist in the cache, this method must generate it with {@link
   * #createCodec(DataType, GenericType, boolean)} (and most likely put it in the cache too for
   * future calls).
   */
  protected abstract TypeCodec<?> getCachedCodec(
      DataType cqlType, GenericType<?> javaType, boolean isJavaCovariant);

  @Override
  public <T> TypeCodec<T> codecFor(DataType cqlType, GenericType<T> javaType) {
    return codecFor(cqlType, javaType, false);
  }

  // Not exposed publicly, (isJavaCovariant=true) is only used for internal recursion
  protected <T> TypeCodec<T> codecFor(
      DataType cqlType, GenericType<T> javaType, boolean isJavaCovariant) {
    LOG.trace("[{}] Looking up codec for {} <-> {}", logPrefix, cqlType, javaType);
    TypeCodec<?> primitiveCodec = primitiveCodecsByCode.get(cqlType.getProtocolCode());
    if (primitiveCodec != null && matches(primitiveCodec, javaType, isJavaCovariant)) {
      LOG.trace("[{}] Found matching primitive codec {}", logPrefix, primitiveCodec);
      return uncheckedCast(primitiveCodec);
    }
    for (TypeCodec<?> userCodec : userCodecs) {
      if (userCodec.accepts(cqlType) && matches(userCodec, javaType, isJavaCovariant)) {
        LOG.trace("[{}] Found matching user codec {}", logPrefix, userCodec);
        return uncheckedCast(userCodec);
      }
    }
    return uncheckedCast(getCachedCodec(cqlType, javaType, isJavaCovariant));
  }

  @Override
  public <T> TypeCodec<T> codecFor(DataType cqlType, Class<T> javaType) {
    LOG.trace("[{}] Looking up codec for {} <-> {}", logPrefix, cqlType, javaType);
    TypeCodec<?> primitiveCodec = primitiveCodecsByCode.get(cqlType.getProtocolCode());
    if (primitiveCodec != null && primitiveCodec.getJavaType().__getToken().getType() == javaType) {
      LOG.trace("[{}] Found matching primitive codec {}", logPrefix, primitiveCodec);
      return uncheckedCast(primitiveCodec);
    }
    for (TypeCodec<?> userCodec : userCodecs) {
      if (userCodec.accepts(cqlType) && userCodec.accepts(javaType)) {
        LOG.trace("[{}] Found matching user codec {}", logPrefix, userCodec);
        return uncheckedCast(userCodec);
      }
    }
    return uncheckedCast(getCachedCodec(cqlType, GenericType.of(javaType), false));
  }

  @Override
  public <T> TypeCodec<T> codecFor(DataType cqlType) {
    LOG.trace("[{}] Looking up codec for CQL type {}", logPrefix, cqlType);
    TypeCodec<?> primitiveCodec = primitiveCodecsByCode.get(cqlType.getProtocolCode());
    if (primitiveCodec != null) {
      LOG.trace("[{}] Found matching primitive codec {}", logPrefix, primitiveCodec);
      return uncheckedCast(primitiveCodec);
    }
    for (TypeCodec<?> userCodec : userCodecs) {
      if (userCodec.accepts(cqlType)) {
        LOG.trace("[{}] Found matching user codec {}", logPrefix, userCodec);
        return uncheckedCast(userCodec);
      }
    }
    return uncheckedCast(getCachedCodec(cqlType, null, false));
  }

  @Override
  public <T> TypeCodec<T> codecFor(DataType cqlType, T value) {
    Preconditions.checkNotNull(cqlType);
    Preconditions.checkNotNull(value);
    LOG.trace("[{}] Looking up codec for CQL type {} and object {}", logPrefix, cqlType, value);

    TypeCodec<?> primitiveCodec = primitiveCodecsByCode.get(cqlType.getProtocolCode());
    if (primitiveCodec != null && primitiveCodec.accepts(value)) {
      LOG.trace("[{}] Found matching primitive codec {}", logPrefix, primitiveCodec);
      return uncheckedCast(primitiveCodec);
    }
    for (TypeCodec<?> userCodec : userCodecs) {
      if (userCodec.accepts(cqlType) && userCodec.accepts(value)) {
        LOG.trace("[{}] Found matching user codec {}", logPrefix, userCodec);
        return uncheckedCast(userCodec);
      }
    }

    if (value instanceof TupleValue) {
      return uncheckedCast(codecFor(cqlType, TupleValue.class));
    } else if (value instanceof UdtValue) {
      return uncheckedCast(codecFor(cqlType, UdtValue.class));
    }

    GenericType<?> javaType = inspectType(value);
    LOG.trace("[{}] Continuing based on inferred type {}", logPrefix, javaType);
    return uncheckedCast(getCachedCodec(cqlType, javaType, true));
  }

  @Override
  public <T> TypeCodec<T> codecFor(T value) {
    Preconditions.checkNotNull(value);
    LOG.trace("[{}] Looking up codec for object {}", logPrefix, value);

    for (TypeCodec<?> primitiveCodec : primitiveCodecs) {
      if (primitiveCodec.accepts(value)) {
        LOG.trace("[{}] Found matching primitive codec {}", logPrefix, primitiveCodec);
        return uncheckedCast(primitiveCodec);
      }
    }
    for (TypeCodec<?> userCodec : userCodecs) {
      if (userCodec.accepts(value)) {
        LOG.trace("[{}] Found matching user codec {}", logPrefix, userCodec);
        return uncheckedCast(userCodec);
      }
    }

    if (value instanceof TupleValue) {
      return uncheckedCast(codecFor(((TupleValue) value).getType(), TupleValue.class));
    } else if (value instanceof UdtValue) {
      return uncheckedCast(codecFor(((UdtValue) value).getType(), UdtValue.class));
    }

    GenericType<?> javaType = inspectType(value);
    LOG.trace("[{}] Continuing based on inferred type {}", logPrefix, javaType);
    return uncheckedCast(getCachedCodec(null, javaType, true));
  }

  @Override
  public <T> TypeCodec<T> codecFor(GenericType<T> javaType) {
    return codecFor(javaType, false);
  }

  // Not exposed publicly, (isJavaCovariant=true) is only used for internal recursion
  protected <T> TypeCodec<T> codecFor(GenericType<T> javaType, boolean isJavaCovariant) {
    LOG.trace(
        "[{}] Looking up codec for Java type {} (covariant = {})",
        logPrefix,
        javaType,
        isJavaCovariant);
    for (TypeCodec<?> primitiveCodec : primitiveCodecs) {
      if (matches(primitiveCodec, javaType, isJavaCovariant)) {
        LOG.trace("[{}] Found matching primitive codec {}", logPrefix, primitiveCodec);
        return uncheckedCast(primitiveCodec);
      }
    }
    for (TypeCodec<?> userCodec : userCodecs) {
      if (matches(userCodec, javaType, isJavaCovariant)) {
        LOG.trace("[{}] Found matching user codec {}", logPrefix, userCodec);
        return uncheckedCast(userCodec);
      }
    }
    return uncheckedCast(getCachedCodec(null, javaType, isJavaCovariant));
  }

  protected boolean matches(TypeCodec<?> codec, GenericType<?> javaType, boolean isJavaCovariant) {
    return (isJavaCovariant)
        ? codec.getJavaType().isSupertypeOf(javaType)
        : codec.accepts(javaType);
  }

  protected GenericType<?> inspectType(Object value) {
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
  protected TypeCodec<?> createCodec(
      DataType cqlType, GenericType<?> javaType, boolean isJavaCovariant) {
    LOG.trace("[{}] Cache miss, creating codec", logPrefix);
    // Either type can be null, but not both.
    if (javaType == null) {
      assert cqlType != null;
      return createCodec(cqlType);
    } else if (cqlType == null) {
      return createCodec(javaType, isJavaCovariant);
    } else { // Both non-null
      TypeToken<?> token = javaType.__getToken();
      if (cqlType instanceof ListType && List.class.isAssignableFrom(token.getRawType())) {
        DataType elementCqlType = ((ListType) cqlType).getElementType();
        TypeCodec<Object> elementCodec;
        if (token.getType() instanceof ParameterizedType) {
          Type[] typeArguments = ((ParameterizedType) token.getType()).getActualTypeArguments();
          GenericType<?> elementJavaType = GenericType.of(typeArguments[0]);
          elementCodec = uncheckedCast(codecFor(elementCqlType, elementJavaType, isJavaCovariant));
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
          elementCodec = uncheckedCast(codecFor(elementCqlType, elementJavaType, isJavaCovariant));
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
          keyCodec = uncheckedCast(codecFor(keyCqlType, keyJavaType, isJavaCovariant));
          valueCodec = uncheckedCast(codecFor(valueCqlType, valueJavaType, isJavaCovariant));
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
  }

  // Try to create a codec when we haven't found it in the cache.
  // Variant where the CQL type is unknown. Can be covariant if we come from a lookup by Java value.
  protected TypeCodec<?> createCodec(GenericType<?> javaType, boolean isJavaCovariant) {
    TypeToken<?> token = javaType.__getToken();
    if (List.class.isAssignableFrom(token.getRawType())
        && token.getType() instanceof ParameterizedType) {
      Type[] typeArguments = ((ParameterizedType) token.getType()).getActualTypeArguments();
      GenericType<?> elementType = GenericType.of(typeArguments[0]);
      TypeCodec<?> elementCodec = codecFor(elementType, isJavaCovariant);
      return TypeCodecs.listOf(elementCodec);
    } else if (Set.class.isAssignableFrom(token.getRawType())
        && token.getType() instanceof ParameterizedType) {
      Type[] typeArguments = ((ParameterizedType) token.getType()).getActualTypeArguments();
      GenericType<?> elementType = GenericType.of(typeArguments[0]);
      TypeCodec<?> elementCodec = codecFor(elementType, isJavaCovariant);
      return TypeCodecs.setOf(elementCodec);
    } else if (Map.class.isAssignableFrom(token.getRawType())
        && token.getType() instanceof ParameterizedType) {
      Type[] typeArguments = ((ParameterizedType) token.getType()).getActualTypeArguments();
      GenericType<?> keyType = GenericType.of(typeArguments[0]);
      GenericType<?> valueType = GenericType.of(typeArguments[1]);
      TypeCodec<?> keyCodec = codecFor(keyType, isJavaCovariant);
      TypeCodec<?> valueCodec = codecFor(valueType, isJavaCovariant);
      return TypeCodecs.mapOf(keyCodec, valueCodec);
    }
    throw new CodecNotFoundException(null, javaType);
  }

  // Try to create a codec when we haven't found it in the cache.
  // Variant where the Java type is unknown.
  protected TypeCodec<?> createCodec(DataType cqlType) {
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

  private static IntMap<TypeCodec> sortByProtocolCode(TypeCodec<?>[] codecs) {
    IntMap.Builder<TypeCodec> builder = IntMap.builder();
    for (TypeCodec<?> codec : codecs) {
      builder.put(codec.getCqlType().getProtocolCode(), codec);
    }
    return builder.build();
  }

  // We call this after validating the types, so we know the cast will never fail.
  private static <T, U> TypeCodec<T> uncheckedCast(TypeCodec<U> codec) {
    @SuppressWarnings("unchecked")
    TypeCodec<T> result = (TypeCodec<T>) codec;
    return result;
  }
}
