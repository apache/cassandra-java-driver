/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.type.codec.registry;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.ContainerType;
import com.datastax.oss.driver.api.core.type.CustomType;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.reflect.TypeToken;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.util.IntMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
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
public abstract class CachingCodecRegistry implements MutableCodecRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(CachingCodecRegistry.class);

  // Implementation notes:
  // - built-in primitive codecs are served directly, without hitting the cache
  // - same for user codecs (we assume the cardinality will always be low, so a sequential array
  //   traversal is cheap).

  protected final String logPrefix;
  private final TypeCodec<?>[] primitiveCodecs;
  private final CopyOnWriteArrayList<TypeCodec<?>> userCodecs = new CopyOnWriteArrayList<>();
  private final IntMap<TypeCodec<?>> primitiveCodecsByCode;
  private final Lock registerLock = new ReentrantLock();

  protected CachingCodecRegistry(
      @NonNull String logPrefix, @NonNull TypeCodec<?>[] primitiveCodecs) {
    this.logPrefix = logPrefix;
    this.primitiveCodecs = primitiveCodecs;
    this.primitiveCodecsByCode = sortByProtocolCode(primitiveCodecs);
  }

  /**
   * @deprecated this constructor calls an overridable method ({@link #register(TypeCodec[])}),
   *     which is a bad practice. The recommended alternative is to use {@link
   *     #CachingCodecRegistry(String, TypeCodec[])}, then add the codecs with one of the {@link
   *     #register} methods.
   */
  @Deprecated
  protected CachingCodecRegistry(
      @NonNull String logPrefix,
      @NonNull TypeCodec<?>[] primitiveCodecs,
      @NonNull TypeCodec<?>[] userCodecs) {
    this(logPrefix, primitiveCodecs);
    register(userCodecs);
  }

  @Override
  public void register(TypeCodec<?> newCodec) {
    // This method could work without synchronization, but there is a tiny race condition that would
    // allow two threads to register colliding codecs (the last added codec would later be ignored,
    // but without any warning). Serialize calls to avoid that:
    registerLock.lock();
    try {
      for (TypeCodec<?> primitiveCodec : primitiveCodecs) {
        if (collides(newCodec, primitiveCodec)) {
          LOG.warn(
              "[{}] Ignoring codec {} because it collides with built-in primitive codec {}",
              logPrefix,
              newCodec,
              primitiveCodec);
          return;
        }
      }
      for (TypeCodec<?> userCodec : userCodecs) {
        if (collides(newCodec, userCodec)) {
          LOG.warn(
              "[{}] Ignoring codec {} because it collides with previously registered codec {}",
              logPrefix,
              newCodec,
              userCodec);
          return;
        }
      }
      // Technically this would cover the two previous cases as well, but we want precise messages.
      try {
        TypeCodec<?> cachedCodec =
            getCachedCodec(newCodec.getCqlType(), newCodec.getJavaType(), false);
        LOG.warn(
            "[{}] Ignoring codec {} because it collides with previously generated codec {}",
            logPrefix,
            newCodec,
            cachedCodec);
        return;
      } catch (CodecNotFoundException ignored) {
        // Catching the exception is ugly, but it avoids breaking the internal API (e.g. by adding a
        // getCachedCodecIfExists)
      }
      userCodecs.add(newCodec);
    } finally {
      registerLock.unlock();
    }
  }

  private boolean collides(TypeCodec<?> newCodec, TypeCodec<?> oldCodec) {
    return oldCodec.accepts(newCodec.getCqlType()) && oldCodec.accepts(newCodec.getJavaType());
  }

  /**
   * Gets a complex codec from the cache.
   *
   * <p>If the codec does not exist in the cache, this method must generate it with {@link
   * #createCodec(DataType, GenericType, boolean)} (and most likely put it in the cache too for
   * future calls).
   */
  protected abstract TypeCodec<?> getCachedCodec(
      @Nullable DataType cqlType, @Nullable GenericType<?> javaType, boolean isJavaCovariant);

  @NonNull
  @Override
  public <JavaTypeT> TypeCodec<JavaTypeT> codecFor(
      @NonNull DataType cqlType, @NonNull GenericType<JavaTypeT> javaType) {
    return codecFor(cqlType, javaType, false);
  }

  // Not exposed publicly, (isJavaCovariant=true) is only used for internal recursion
  @NonNull
  protected <JavaTypeT> TypeCodec<JavaTypeT> codecFor(
      @NonNull DataType cqlType,
      @NonNull GenericType<JavaTypeT> javaType,
      boolean isJavaCovariant) {
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

  @NonNull
  @Override
  public <JavaTypeT> TypeCodec<JavaTypeT> codecFor(
      @NonNull DataType cqlType, @NonNull Class<JavaTypeT> javaType) {
    LOG.trace("[{}] Looking up codec for {} <-> {}", logPrefix, cqlType, javaType);
    TypeCodec<?> primitiveCodec = primitiveCodecsByCode.get(cqlType.getProtocolCode());
    if (primitiveCodec != null && primitiveCodec.accepts(javaType)) {
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

  @NonNull
  @Override
  public <JavaTypeT> TypeCodec<JavaTypeT> codecFor(@NonNull DataType cqlType) {
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

  @NonNull
  @Override
  public <JavaTypeT> TypeCodec<JavaTypeT> codecFor(
      @NonNull DataType cqlType, @NonNull JavaTypeT value) {
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

    GenericType<?> javaType = inspectType(value, cqlType);
    LOG.trace("[{}] Continuing based on inferred type {}", logPrefix, javaType);
    return uncheckedCast(getCachedCodec(cqlType, javaType, true));
  }

  @NonNull
  @Override
  public <JavaTypeT> TypeCodec<JavaTypeT> codecFor(@NonNull JavaTypeT value) {
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

    DataType cqlType = inferCqlTypeFromValue(value);
    GenericType<?> javaType = inspectType(value, cqlType);
    LOG.trace(
        "[{}] Continuing based on inferred CQL type {} and Java type {}",
        logPrefix,
        cqlType,
        javaType);
    return uncheckedCast(getCachedCodec(cqlType, javaType, true));
  }

  @NonNull
  @Override
  public <JavaTypeT> TypeCodec<JavaTypeT> codecFor(@NonNull GenericType<JavaTypeT> javaType) {
    return codecFor(javaType, false);
  }

  // Not exposed publicly, (isJavaCovariant=true) is only used for internal recursion
  @NonNull
  protected <JavaTypeT> TypeCodec<JavaTypeT> codecFor(
      @NonNull GenericType<JavaTypeT> javaType, boolean isJavaCovariant) {
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

  protected boolean matches(
      @NonNull TypeCodec<?> codec, @NonNull GenericType<?> javaType, boolean isJavaCovariant) {
    return isJavaCovariant ? codec.getJavaType().isSupertypeOf(javaType) : codec.accepts(javaType);
  }

  @NonNull
  protected GenericType<?> inspectType(@NonNull Object value, @Nullable DataType cqlType) {
    if (value instanceof List) {
      List<?> list = (List<?>) value;
      if (list.isEmpty()) {
        // Empty collections are always encoded the same way, so any element type will do
        // in the absence of a CQL type. When the CQL type is known, we try to infer the best Java
        // type.
        return cqlType == null ? JAVA_TYPE_FOR_EMPTY_LISTS : inferJavaTypeFromCqlType(cqlType);
      } else {
        Object firstElement = list.get(0);
        if (firstElement == null) {
          throw new IllegalArgumentException(
              "Can't infer list codec because the first element is null "
                  + "(note that CQL does not allow null values in collections)");
        }
        GenericType<?> elementType =
            inspectType(
                firstElement, cqlType == null ? null : ((ContainerType) cqlType).getElementType());
        return GenericType.listOf(elementType);
      }
    } else if (value instanceof Set) {
      Set<?> set = (Set<?>) value;
      if (set.isEmpty()) {
        return cqlType == null ? JAVA_TYPE_FOR_EMPTY_SETS : inferJavaTypeFromCqlType(cqlType);
      } else {
        Object firstElement = set.iterator().next();
        if (firstElement == null) {
          throw new IllegalArgumentException(
              "Can't infer set codec because the first element is null "
                  + "(note that CQL does not allow null values in collections)");
        }
        GenericType<?> elementType =
            inspectType(
                firstElement, cqlType == null ? null : ((SetType) cqlType).getElementType());
        return GenericType.setOf(elementType);
      }
    } else if (value instanceof Map) {
      Map<?, ?> map = (Map<?, ?>) value;
      if (map.isEmpty()) {
        return cqlType == null ? JAVA_TYPE_FOR_EMPTY_MAPS : inferJavaTypeFromCqlType(cqlType);
      } else {
        Map.Entry<?, ?> firstEntry = map.entrySet().iterator().next();
        Object firstKey = firstEntry.getKey();
        Object firstValue = firstEntry.getValue();
        if (firstKey == null || firstValue == null) {
          throw new IllegalArgumentException(
              "Can't infer map codec because the first key and/or value is null "
                  + "(note that CQL does not allow null values in collections)");
        }
        GenericType<?> keyType =
            inspectType(firstKey, cqlType == null ? null : ((MapType) cqlType).getKeyType());
        GenericType<?> valueType =
            inspectType(firstValue, cqlType == null ? null : ((MapType) cqlType).getValueType());
        return GenericType.mapOf(keyType, valueType);
      }
    } else if (value instanceof CqlVector) {
      CqlVector<?> vector = (CqlVector<?>) value;
      if (vector.isEmpty()) {
        return cqlType == null ? JAVA_TYPE_FOR_EMPTY_CQLVECTORS : inferJavaTypeFromCqlType(cqlType);
      } else {
        Object firstElement = vector.iterator().next();
        if (firstElement == null) {
          throw new IllegalArgumentException(
              "Can't infer vector codec because the first element is null "
                  + "(note that CQL does not allow null values in collections)");
        }
        GenericType<?> elementType =
            inspectType(
                firstElement, cqlType == null ? null : ((VectorType) cqlType).getElementType());
        return GenericType.vectorOf(elementType);
      }
    } else {
      // There's not much more we can do
      return GenericType.of(value.getClass());
    }
  }

  @NonNull
  protected GenericType<?> inferJavaTypeFromCqlType(@NonNull DataType cqlType) {
    if (cqlType instanceof ListType) {
      DataType elementType = ((ListType) cqlType).getElementType();
      return GenericType.listOf(inferJavaTypeFromCqlType(elementType));
    } else if (cqlType instanceof SetType) {
      DataType elementType = ((SetType) cqlType).getElementType();
      return GenericType.setOf(inferJavaTypeFromCqlType(elementType));
    } else if (cqlType instanceof MapType) {
      DataType keyType = ((MapType) cqlType).getKeyType();
      DataType valueType = ((MapType) cqlType).getValueType();
      return GenericType.mapOf(
          inferJavaTypeFromCqlType(keyType), inferJavaTypeFromCqlType(valueType));
    } else if (cqlType instanceof VectorType) {
      DataType elementType = ((VectorType) cqlType).getElementType();
      GenericType<?> numberType = inferJavaTypeFromCqlType(elementType);
      return GenericType.vectorOf(numberType);
    }
    switch (cqlType.getProtocolCode()) {
      case ProtocolConstants.DataType.CUSTOM:
      case ProtocolConstants.DataType.BLOB:
        return GenericType.BYTE_BUFFER;
      case ProtocolConstants.DataType.ASCII:
      case ProtocolConstants.DataType.VARCHAR:
        return GenericType.STRING;
      case ProtocolConstants.DataType.BIGINT:
      case ProtocolConstants.DataType.COUNTER:
        return GenericType.LONG;
      case ProtocolConstants.DataType.BOOLEAN:
        return GenericType.BOOLEAN;
      case ProtocolConstants.DataType.DECIMAL:
        return GenericType.BIG_DECIMAL;
      case ProtocolConstants.DataType.DOUBLE:
        return GenericType.DOUBLE;
      case ProtocolConstants.DataType.FLOAT:
        return GenericType.FLOAT;
      case ProtocolConstants.DataType.INT:
        return GenericType.INTEGER;
      case ProtocolConstants.DataType.TIMESTAMP:
        return GenericType.INSTANT;
      case ProtocolConstants.DataType.UUID:
      case ProtocolConstants.DataType.TIMEUUID:
        return GenericType.UUID;
      case ProtocolConstants.DataType.VARINT:
        return GenericType.BIG_INTEGER;
      case ProtocolConstants.DataType.INET:
        return GenericType.INET_ADDRESS;
      case ProtocolConstants.DataType.DATE:
        return GenericType.LOCAL_DATE;
      case ProtocolConstants.DataType.TIME:
        return GenericType.LOCAL_TIME;
      case ProtocolConstants.DataType.SMALLINT:
        return GenericType.SHORT;
      case ProtocolConstants.DataType.TINYINT:
        return GenericType.BYTE;
      case ProtocolConstants.DataType.DURATION:
        return GenericType.CQL_DURATION;
      case ProtocolConstants.DataType.UDT:
        return GenericType.UDT_VALUE;
      case ProtocolConstants.DataType.TUPLE:
        return GenericType.TUPLE_VALUE;
      default:
        throw new CodecNotFoundException(cqlType, null);
    }
  }

  @Nullable
  protected DataType inferCqlTypeFromValue(@NonNull Object value) {
    if (value instanceof List) {
      List<?> list = (List<?>) value;
      if (list.isEmpty()) {
        return CQL_TYPE_FOR_EMPTY_LISTS;
      }
      Object firstElement = list.get(0);
      if (firstElement == null) {
        throw new IllegalArgumentException(
            "Can't infer list codec because the first element is null "
                + "(note that CQL does not allow null values in collections)");
      }
      DataType elementType = inferCqlTypeFromValue(firstElement);
      if (elementType == null) {
        return null;
      }
      return DataTypes.listOf(elementType);
    } else if (value instanceof Set) {
      Set<?> set = (Set<?>) value;
      if (set.isEmpty()) {
        return CQL_TYPE_FOR_EMPTY_SETS;
      }
      Object firstElement = set.iterator().next();
      if (firstElement == null) {
        throw new IllegalArgumentException(
            "Can't infer set codec because the first element is null "
                + "(note that CQL does not allow null values in collections)");
      }
      DataType elementType = inferCqlTypeFromValue(firstElement);
      if (elementType == null) {
        return null;
      }
      return DataTypes.setOf(elementType);
    } else if (value instanceof Map) {
      Map<?, ?> map = (Map<?, ?>) value;
      if (map.isEmpty()) {
        return CQL_TYPE_FOR_EMPTY_MAPS;
      }
      Entry<?, ?> firstEntry = map.entrySet().iterator().next();
      Object firstKey = firstEntry.getKey();
      Object firstValue = firstEntry.getValue();
      if (firstKey == null || firstValue == null) {
        throw new IllegalArgumentException(
            "Can't infer map codec because the first key and/or value is null "
                + "(note that CQL does not allow null values in collections)");
      }
      DataType keyType = inferCqlTypeFromValue(firstKey);
      DataType valueType = inferCqlTypeFromValue(firstValue);
      if (keyType == null || valueType == null) {
        return null;
      }
      return DataTypes.mapOf(keyType, valueType);
    } else if (value instanceof CqlVector) {
      CqlVector<?> vector = (CqlVector<?>) value;
      if (vector.isEmpty()) {
        return CQL_TYPE_FOR_EMPTY_VECTORS;
      }
      Object firstElement = vector.iterator().next();
      if (firstElement == null) {
        throw new IllegalArgumentException(
            "Can't infer vector codec because the first element is null "
                + "(note that CQL does not allow null values in collections)");
      }
      DataType elementType = inferCqlTypeFromValue(firstElement);
      if (elementType == null) {
        return null;
      }
      return DataTypes.vectorOf(elementType, vector.size());
    }
    Class<?> javaClass = value.getClass();
    if (ByteBuffer.class.isAssignableFrom(javaClass)) {
      return DataTypes.BLOB;
    } else if (String.class.equals(javaClass)) {
      return DataTypes.TEXT;
    } else if (Long.class.equals(javaClass)) {
      return DataTypes.BIGINT;
    } else if (Boolean.class.equals(javaClass)) {
      return DataTypes.BOOLEAN;
    } else if (BigDecimal.class.equals(javaClass)) {
      return DataTypes.DECIMAL;
    } else if (Double.class.equals(javaClass)) {
      return DataTypes.DOUBLE;
    } else if (Float.class.equals(javaClass)) {
      return DataTypes.FLOAT;
    } else if (Integer.class.equals(javaClass)) {
      return DataTypes.INT;
    } else if (Instant.class.equals(javaClass)) {
      return DataTypes.TIMESTAMP;
    } else if (UUID.class.equals(javaClass)) {
      return DataTypes.UUID;
    } else if (BigInteger.class.equals(javaClass)) {
      return DataTypes.VARINT;
    } else if (InetAddress.class.isAssignableFrom(javaClass)) {
      return DataTypes.INET;
    } else if (LocalDate.class.equals(javaClass)) {
      return DataTypes.DATE;
    } else if (LocalTime.class.equals(javaClass)) {
      return DataTypes.TIME;
    } else if (Short.class.equals(javaClass)) {
      return DataTypes.SMALLINT;
    } else if (Byte.class.equals(javaClass)) {
      return DataTypes.TINYINT;
    } else if (CqlDuration.class.equals(javaClass)) {
      return DataTypes.DURATION;
    } else if (UdtValue.class.isAssignableFrom(javaClass)) {
      return ((UdtValue) value).getType();
    } else if (TupleValue.class.isAssignableFrom(javaClass)) {
      return ((TupleValue) value).getType();
    }
    // This might mean that the java type is a custom type with a custom codec,
    // so don't throw CodecNotFoundException just yet.
    return null;
  }

  private TypeCodec<Object> getElementCodecForCqlAndJavaType(
      ContainerType cqlType, TypeToken<?> token, boolean isJavaCovariant) {

    DataType elementCqlType = cqlType.getElementType();
    if (token.getType() instanceof ParameterizedType) {
      Type[] typeArguments = ((ParameterizedType) token.getType()).getActualTypeArguments();
      GenericType<?> elementJavaType = GenericType.of(typeArguments[0]);
      return uncheckedCast(codecFor(elementCqlType, elementJavaType, isJavaCovariant));
    }
    return codecFor(elementCqlType);
  }

  private TypeCodec<?> getElementCodecForJavaType(
      ParameterizedType parameterizedType, boolean isJavaCovariant) {

    Type[] typeArguments = parameterizedType.getActualTypeArguments();
    GenericType<?> elementType = GenericType.of(typeArguments[0]);
    return codecFor(elementType, isJavaCovariant);
  }

  // Try to create a codec when we haven't found it in the cache
  @NonNull
  protected TypeCodec<?> createCodec(
      @Nullable DataType cqlType, @Nullable GenericType<?> javaType, boolean isJavaCovariant) {
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
        TypeCodec<Object> elementCodec =
            getElementCodecForCqlAndJavaType((ContainerType) cqlType, token, isJavaCovariant);
        return TypeCodecs.listOf(elementCodec);
      } else if (cqlType instanceof SetType && Set.class.isAssignableFrom(token.getRawType())) {
        TypeCodec<Object> elementCodec =
            getElementCodecForCqlAndJavaType((ContainerType) cqlType, token, isJavaCovariant);
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
      } else if (cqlType instanceof VectorType
          && CqlVector.class.isAssignableFrom(token.getRawType())) {
        VectorType vectorType = (VectorType) cqlType;
        /* For a vector type we'll always get back an instance of TypeCodec<? extends Number> due to the
         * type of CqlVector... but getElementCodecForCqlAndJavaType() is a generalized function that can't
         * return this more precise type.  Thus the cast here. */
        TypeCodec<?> elementCodec =
            uncheckedCast(getElementCodecForCqlAndJavaType(vectorType, token, isJavaCovariant));
        return TypeCodecs.vectorOf(vectorType, elementCodec);
      } else if (cqlType instanceof CustomType
          && ByteBuffer.class.isAssignableFrom(token.getRawType())) {
        return TypeCodecs.custom(cqlType);
      }
      throw new CodecNotFoundException(cqlType, javaType);
    }
  }

  // Try to create a codec when we haven't found it in the cache.
  // Variant where the CQL type is unknown. Can be covariant if we come from a lookup by Java value.
  @NonNull
  protected TypeCodec<?> createCodec(@NonNull GenericType<?> javaType, boolean isJavaCovariant) {
    TypeToken<?> token = javaType.__getToken();
    if (List.class.isAssignableFrom(token.getRawType())
        && token.getType() instanceof ParameterizedType) {
      TypeCodec<?> elementCodec =
          getElementCodecForJavaType((ParameterizedType) token.getType(), isJavaCovariant);
      return TypeCodecs.listOf(elementCodec);
    } else if (Set.class.isAssignableFrom(token.getRawType())
        && token.getType() instanceof ParameterizedType) {
      TypeCodec<?> elementCodec =
          getElementCodecForJavaType((ParameterizedType) token.getType(), isJavaCovariant);
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
    /* Note that this method cannot generate TypeCodec instances for any CqlVector type.  VectorCodec needs
     * to know the dimensions of the vector it will be operating on and there's no way to determine that from
     * the Java type alone. */
    throw new CodecNotFoundException(null, javaType);
  }

  // Try to create a codec when we haven't found it in the cache.
  // Variant where the Java type is unknown.
  @NonNull
  protected TypeCodec<?> createCodec(@NonNull DataType cqlType) {
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
    } else if (cqlType instanceof VectorType) {
      VectorType vectorType = (VectorType) cqlType;
      TypeCodec<? extends Number> elementCodec =
          uncheckedCast(codecFor(vectorType.getElementType()));
      return TypeCodecs.vectorOf(vectorType, elementCodec);
    } else if (cqlType instanceof TupleType) {
      return TypeCodecs.tupleOf((TupleType) cqlType);
    } else if (cqlType instanceof UserDefinedType) {
      return TypeCodecs.udtOf((UserDefinedType) cqlType);
    } else if (cqlType instanceof CustomType) {
      return TypeCodecs.custom(cqlType);
    }
    throw new CodecNotFoundException(cqlType, null);
  }

  private static IntMap<TypeCodec<?>> sortByProtocolCode(TypeCodec<?>[] codecs) {
    IntMap.Builder<TypeCodec<?>> builder = IntMap.builder();
    for (TypeCodec<?> codec : codecs) {
      builder.put(codec.getCqlType().getProtocolCode(), codec);
    }
    return builder.build();
  }

  // We call this after validating the types, so we know the cast will never fail.
  private static <DeclaredT, RuntimeT> TypeCodec<DeclaredT> uncheckedCast(
      TypeCodec<RuntimeT> codec) {
    @SuppressWarnings("unchecked")
    TypeCodec<DeclaredT> result = (TypeCodec<DeclaredT>) codec;
    return result;
  }

  // These are mock types that are used as placeholders when we try to find a codec for an empty
  // Java collection instance. All empty collections are serialized in the same way, so any element
  // type will do:
  private static final GenericType<List<Boolean>> JAVA_TYPE_FOR_EMPTY_LISTS =
      GenericType.listOf(Boolean.class);
  private static final GenericType<Set<Boolean>> JAVA_TYPE_FOR_EMPTY_SETS =
      GenericType.setOf(Boolean.class);
  private static final GenericType<Map<Boolean, Boolean>> JAVA_TYPE_FOR_EMPTY_MAPS =
      GenericType.mapOf(Boolean.class, Boolean.class);
  private static final GenericType<CqlVector<Number>> JAVA_TYPE_FOR_EMPTY_CQLVECTORS =
      GenericType.vectorOf(Number.class);
  private static final DataType CQL_TYPE_FOR_EMPTY_LISTS = DataTypes.listOf(DataTypes.BOOLEAN);
  private static final DataType CQL_TYPE_FOR_EMPTY_SETS = DataTypes.setOf(DataTypes.BOOLEAN);
  private static final DataType CQL_TYPE_FOR_EMPTY_MAPS =
      DataTypes.mapOf(DataTypes.BOOLEAN, DataTypes.BOOLEAN);
  private static final DataType CQL_TYPE_FOR_EMPTY_VECTORS = DataTypes.vectorOf(DataTypes.INT, 0);
}
