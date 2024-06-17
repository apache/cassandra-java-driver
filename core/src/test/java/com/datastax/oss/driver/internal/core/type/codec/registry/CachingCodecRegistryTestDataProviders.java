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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.data.DefaultTupleValue;
import com.datastax.oss.driver.internal.core.data.DefaultUdtValue;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.tngtech.java.junit.dataprovider.DataProvider;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Collections;
import java.util.UUID;

@SuppressWarnings("unused")
public class CachingCodecRegistryTestDataProviders {

  @DataProvider
  public static Object[][] primitiveCodecs() {
    return new Object[][] {
      {TypeCodecs.BOOLEAN},
      {TypeCodecs.TINYINT},
      {TypeCodecs.DOUBLE},
      {TypeCodecs.COUNTER},
      {TypeCodecs.FLOAT},
      {TypeCodecs.INT},
      {TypeCodecs.BIGINT},
      {TypeCodecs.SMALLINT},
      {TypeCodecs.TIMESTAMP},
      {TypeCodecs.DATE},
      {TypeCodecs.TIME},
      {TypeCodecs.BLOB},
      {TypeCodecs.TEXT},
      {TypeCodecs.ASCII},
      {TypeCodecs.VARINT},
      {TypeCodecs.DECIMAL},
      {TypeCodecs.UUID},
      {TypeCodecs.TIMEUUID},
      {TypeCodecs.INET},
      {TypeCodecs.DURATION},
    };
  }

  @DataProvider
  public static Object[][] primitiveCodecsWithValues() throws UnknownHostException {
    return new Object[][] {
      {true, TypeCodecs.BOOLEAN},
      {(byte) 0, TypeCodecs.TINYINT},
      {0.0, TypeCodecs.DOUBLE},
      {0.0f, TypeCodecs.FLOAT},
      {0, TypeCodecs.INT},
      {0L, TypeCodecs.BIGINT},
      {(short) 0, TypeCodecs.SMALLINT},
      {Instant.EPOCH, TypeCodecs.TIMESTAMP},
      {LocalDate.MIN, TypeCodecs.DATE},
      {LocalTime.MIDNIGHT, TypeCodecs.TIME},
      {ByteBuffer.allocate(0), TypeCodecs.BLOB},
      {"", TypeCodecs.TEXT},
      {BigInteger.ONE, TypeCodecs.VARINT},
      {BigDecimal.ONE, TypeCodecs.DECIMAL},
      {new UUID(2L, 1L), TypeCodecs.UUID},
      {InetAddress.getByName("127.0.0.1"), TypeCodecs.INET},
      {CqlDuration.newInstance(1, 2, 3), TypeCodecs.DURATION},
    };
  }

  @DataProvider
  public static Object[][] primitiveCodecsWithCqlTypesAndValues() throws UnknownHostException {
    return new Object[][] {
      {DataTypes.BOOLEAN, true, TypeCodecs.BOOLEAN},
      {DataTypes.TINYINT, (byte) 0, TypeCodecs.TINYINT},
      {DataTypes.DOUBLE, 0.0, TypeCodecs.DOUBLE},
      {DataTypes.FLOAT, 0.0f, TypeCodecs.FLOAT},
      {DataTypes.INT, 0, TypeCodecs.INT},
      {DataTypes.BIGINT, 0L, TypeCodecs.BIGINT},
      {DataTypes.SMALLINT, (short) 0, TypeCodecs.SMALLINT},
      {DataTypes.TIMESTAMP, Instant.EPOCH, TypeCodecs.TIMESTAMP},
      {DataTypes.DATE, LocalDate.MIN, TypeCodecs.DATE},
      {DataTypes.TIME, LocalTime.MIDNIGHT, TypeCodecs.TIME},
      {DataTypes.BLOB, ByteBuffer.allocate(0), TypeCodecs.BLOB},
      {DataTypes.TEXT, "", TypeCodecs.TEXT},
      {DataTypes.VARINT, BigInteger.ONE, TypeCodecs.VARINT},
      {DataTypes.DECIMAL, BigDecimal.ONE, TypeCodecs.DECIMAL},
      {DataTypes.UUID, new UUID(2L, 1L), TypeCodecs.UUID},
      {DataTypes.INET, InetAddress.getByName("127.0.0.1"), TypeCodecs.INET},
      {DataTypes.DURATION, CqlDuration.newInstance(1, 2, 3), TypeCodecs.DURATION},
    };
  }

  @DataProvider
  public static Object[][] collectionsWithCqlAndJavaTypes()
      throws UnknownHostException, ClassNotFoundException {
    TupleType tupleType = DataTypes.tupleOf(DataTypes.INT, DataTypes.listOf(DataTypes.TEXT));
    TupleValue tupleValue = tupleType.newValue();
    UserDefinedType userType =
        new UserDefinedTypeBuilder(
                CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("type"))
            .withField(CqlIdentifier.fromInternal("field1"), DataTypes.INT)
            .withField(CqlIdentifier.fromInternal("field2"), DataTypes.listOf(DataTypes.TEXT))
            .build();
    UdtValue udtValue = userType.newValue();
    return new Object[][] {
      // lists
      {
        DataTypes.listOf(DataTypes.INT),
        GenericType.listOf(Integer.class),
        GenericType.listOf(Integer.class),
        ImmutableList.of(1)
      },
      {
        DataTypes.listOf(DataTypes.TEXT),
        GenericType.listOf(String.class),
        GenericType.listOf(String.class),
        ImmutableList.of("foo")
      },
      {
        DataTypes.listOf(DataTypes.BLOB),
        GenericType.listOf(ByteBuffer.class),
        GenericType.listOf(Class.forName("java.nio.HeapByteBuffer")),
        ImmutableList.of(ByteBuffer.wrap(new byte[] {127, 0, 0, 1}))
      },
      {
        DataTypes.listOf(DataTypes.INET),
        GenericType.listOf(InetAddress.class),
        GenericType.listOf(Inet4Address.class),
        ImmutableList.of(InetAddress.getByAddress(new byte[] {127, 0, 0, 1}))
      },
      {
        DataTypes.listOf(tupleType),
        GenericType.listOf(TupleValue.class),
        GenericType.listOf(DefaultTupleValue.class),
        ImmutableList.of(tupleValue)
      },
      {
        DataTypes.listOf(userType),
        GenericType.listOf(UdtValue.class),
        GenericType.listOf(DefaultUdtValue.class),
        ImmutableList.of(udtValue)
      },
      {
        DataTypes.listOf(DataTypes.listOf(DataTypes.INT)),
        GenericType.listOf(GenericType.listOf(Integer.class)),
        GenericType.listOf(GenericType.listOf(Integer.class)),
        ImmutableList.of(ImmutableList.of(1))
      },
      {
        DataTypes.listOf(DataTypes.listOf(tupleType)),
        GenericType.listOf(GenericType.listOf(TupleValue.class)),
        GenericType.listOf(GenericType.listOf(DefaultTupleValue.class)),
        ImmutableList.of(ImmutableList.of(tupleValue))
      },
      {
        DataTypes.listOf(DataTypes.listOf(userType)),
        GenericType.listOf(GenericType.listOf(UdtValue.class)),
        GenericType.listOf(GenericType.listOf(DefaultUdtValue.class)),
        ImmutableList.of(ImmutableList.of(udtValue))
      },
      // sets
      {
        DataTypes.setOf(DataTypes.INT),
        GenericType.setOf(Integer.class),
        GenericType.setOf(Integer.class),
        ImmutableSet.of(1)
      },
      {
        DataTypes.setOf(DataTypes.TEXT),
        GenericType.setOf(String.class),
        GenericType.setOf(String.class),
        ImmutableSet.of("foo")
      },
      {
        DataTypes.setOf(DataTypes.BLOB),
        GenericType.setOf(ByteBuffer.class),
        GenericType.setOf(Class.forName("java.nio.HeapByteBuffer")),
        ImmutableSet.of(ByteBuffer.wrap(new byte[] {127, 0, 0, 1}))
      },
      {
        DataTypes.setOf(DataTypes.INET),
        GenericType.setOf(InetAddress.class),
        GenericType.setOf(Inet4Address.class),
        ImmutableSet.of(InetAddress.getByAddress(new byte[] {127, 0, 0, 1}))
      },
      {
        DataTypes.setOf(tupleType),
        GenericType.setOf(TupleValue.class),
        GenericType.setOf(DefaultTupleValue.class),
        ImmutableSet.of(tupleValue)
      },
      {
        DataTypes.setOf(userType),
        GenericType.setOf(UdtValue.class),
        GenericType.setOf(DefaultUdtValue.class),
        ImmutableSet.of(udtValue)
      },
      {
        DataTypes.setOf(DataTypes.setOf(DataTypes.INT)),
        GenericType.setOf(GenericType.setOf(Integer.class)),
        GenericType.setOf(GenericType.setOf(Integer.class)),
        ImmutableSet.of(ImmutableSet.of(1))
      },
      {
        DataTypes.setOf(DataTypes.setOf(tupleType)),
        GenericType.setOf(GenericType.setOf(TupleValue.class)),
        GenericType.setOf(GenericType.setOf(DefaultTupleValue.class)),
        ImmutableSet.of(ImmutableSet.of(tupleValue))
      },
      {
        DataTypes.setOf(DataTypes.setOf(userType)),
        GenericType.setOf(GenericType.setOf(UdtValue.class)),
        GenericType.setOf(GenericType.setOf(DefaultUdtValue.class)),
        ImmutableSet.of(ImmutableSet.of(udtValue))
      },
      // maps
      {
        DataTypes.mapOf(DataTypes.INT, DataTypes.TEXT),
        GenericType.mapOf(Integer.class, String.class),
        GenericType.mapOf(Integer.class, String.class),
        ImmutableMap.of(1, "foo")
      },
      {
        DataTypes.mapOf(DataTypes.BLOB, DataTypes.INET),
        GenericType.mapOf(ByteBuffer.class, InetAddress.class),
        GenericType.mapOf(Class.forName("java.nio.HeapByteBuffer"), Inet4Address.class),
        ImmutableMap.of(
            ByteBuffer.wrap(new byte[] {127, 0, 0, 1}),
            InetAddress.getByAddress(new byte[] {127, 0, 0, 1}))
      },
      {
        DataTypes.mapOf(tupleType, tupleType),
        GenericType.mapOf(TupleValue.class, TupleValue.class),
        GenericType.mapOf(DefaultTupleValue.class, DefaultTupleValue.class),
        ImmutableMap.of(tupleValue, tupleValue)
      },
      {
        DataTypes.mapOf(userType, userType),
        GenericType.mapOf(UdtValue.class, UdtValue.class),
        GenericType.mapOf(DefaultUdtValue.class, DefaultUdtValue.class),
        ImmutableMap.of(udtValue, udtValue)
      },
      {
        DataTypes.mapOf(DataTypes.UUID, DataTypes.mapOf(DataTypes.INT, DataTypes.TEXT)),
        GenericType.mapOf(GenericType.UUID, GenericType.mapOf(Integer.class, String.class)),
        GenericType.mapOf(GenericType.UUID, GenericType.mapOf(Integer.class, String.class)),
        ImmutableMap.of(UUID.randomUUID(), ImmutableMap.of(1, "foo"))
      },
      {
        DataTypes.mapOf(DataTypes.mapOf(userType, userType), DataTypes.mapOf(tupleType, tupleType)),
        GenericType.mapOf(
            GenericType.mapOf(UdtValue.class, UdtValue.class),
            GenericType.mapOf(TupleValue.class, TupleValue.class)),
        GenericType.mapOf(
            GenericType.mapOf(DefaultUdtValue.class, DefaultUdtValue.class),
            GenericType.mapOf(DefaultTupleValue.class, DefaultTupleValue.class)),
        ImmutableMap.of(
            ImmutableMap.of(udtValue, udtValue), ImmutableMap.of(tupleValue, tupleValue))
      },
      // vectors
      {
        DataTypes.vectorOf(DataTypes.INT, 1),
        GenericType.vectorOf(Integer.class),
        GenericType.vectorOf(Integer.class),
        CqlVector.newInstance(1)
      },
      {
        DataTypes.vectorOf(DataTypes.BIGINT, 1),
        GenericType.vectorOf(Long.class),
        GenericType.vectorOf(Long.class),
        CqlVector.newInstance(1l)
      },
      {
        DataTypes.vectorOf(DataTypes.SMALLINT, 1),
        GenericType.vectorOf(Short.class),
        GenericType.vectorOf(Short.class),
        CqlVector.newInstance((short) 1)
      },
      {
        DataTypes.vectorOf(DataTypes.TINYINT, 1),
        GenericType.vectorOf(Byte.class),
        GenericType.vectorOf(Byte.class),
        CqlVector.newInstance((byte) 1)
      },
      {
        DataTypes.vectorOf(DataTypes.FLOAT, 1),
        GenericType.vectorOf(Float.class),
        GenericType.vectorOf(Float.class),
        CqlVector.newInstance(1.0f)
      },
      {
        DataTypes.vectorOf(DataTypes.DOUBLE, 1),
        GenericType.vectorOf(Double.class),
        GenericType.vectorOf(Double.class),
        CqlVector.newInstance(1.0d)
      },
      {
        DataTypes.vectorOf(DataTypes.DECIMAL, 1),
        GenericType.vectorOf(BigDecimal.class),
        GenericType.vectorOf(BigDecimal.class),
        CqlVector.newInstance(BigDecimal.ONE)
      },
      {
        DataTypes.vectorOf(DataTypes.VARINT, 1),
        GenericType.vectorOf(BigInteger.class),
        GenericType.vectorOf(BigInteger.class),
        CqlVector.newInstance(BigInteger.ONE)
      },
    };
  }

  @DataProvider
  public static Object[][] emptyCollectionsWithCqlAndJavaTypes() {
    TupleType tupleType = DataTypes.tupleOf(DataTypes.INT, DataTypes.listOf(DataTypes.TEXT));
    UserDefinedType userType =
        new UserDefinedTypeBuilder(
                CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("type"))
            .withField(CqlIdentifier.fromInternal("field1"), DataTypes.INT)
            .withField(CqlIdentifier.fromInternal("field2"), DataTypes.listOf(DataTypes.TEXT))
            .build();
    return new Object[][] {
      // lists
      {
        DataTypes.listOf(DataTypes.INT),
        GenericType.listOf(Integer.class),
        DataTypes.listOf(DataTypes.BOOLEAN),
        GenericType.listOf(Boolean.class),
        Collections.emptyList()
      },
      {
        DataTypes.listOf(DataTypes.TEXT),
        GenericType.listOf(String.class),
        DataTypes.listOf(DataTypes.BOOLEAN),
        GenericType.listOf(Boolean.class),
        Collections.emptyList()
      },
      {
        DataTypes.listOf(DataTypes.BLOB),
        GenericType.listOf(ByteBuffer.class),
        DataTypes.listOf(DataTypes.BOOLEAN),
        GenericType.listOf(Boolean.class),
        Collections.emptyList()
      },
      {
        DataTypes.listOf(DataTypes.INET),
        GenericType.listOf(InetAddress.class),
        DataTypes.listOf(DataTypes.BOOLEAN),
        GenericType.listOf(Boolean.class),
        Collections.emptyList()
      },
      {
        DataTypes.listOf(tupleType),
        GenericType.listOf(TupleValue.class),
        DataTypes.listOf(DataTypes.BOOLEAN),
        GenericType.listOf(Boolean.class),
        Collections.emptyList()
      },
      {
        DataTypes.listOf(userType),
        GenericType.listOf(UdtValue.class),
        DataTypes.listOf(DataTypes.BOOLEAN),
        GenericType.listOf(Boolean.class),
        Collections.emptyList()
      },
      {
        DataTypes.listOf(DataTypes.listOf(DataTypes.INT)),
        GenericType.listOf(GenericType.listOf(Integer.class)),
        DataTypes.listOf(DataTypes.listOf(DataTypes.BOOLEAN)),
        GenericType.listOf(GenericType.listOf(Boolean.class)),
        ImmutableList.of(Collections.emptyList())
      },
      {
        DataTypes.listOf(DataTypes.listOf(tupleType)),
        GenericType.listOf(GenericType.listOf(TupleValue.class)),
        DataTypes.listOf(DataTypes.listOf(DataTypes.BOOLEAN)),
        GenericType.listOf(GenericType.listOf(Boolean.class)),
        ImmutableList.of(Collections.emptyList())
      },
      {
        DataTypes.listOf(DataTypes.listOf(userType)),
        GenericType.listOf(GenericType.listOf(UdtValue.class)),
        DataTypes.listOf(DataTypes.listOf(DataTypes.BOOLEAN)),
        GenericType.listOf(GenericType.listOf(Boolean.class)),
        ImmutableList.of(Collections.emptyList())
      },
      // sets
      {
        DataTypes.setOf(DataTypes.INT),
        GenericType.setOf(Integer.class),
        DataTypes.setOf(DataTypes.BOOLEAN),
        GenericType.setOf(Boolean.class),
        Collections.emptySet()
      },
      {
        DataTypes.setOf(DataTypes.TEXT),
        GenericType.setOf(String.class),
        DataTypes.setOf(DataTypes.BOOLEAN),
        GenericType.setOf(Boolean.class),
        Collections.emptySet()
      },
      {
        DataTypes.setOf(DataTypes.BLOB),
        GenericType.setOf(ByteBuffer.class),
        DataTypes.setOf(DataTypes.BOOLEAN),
        GenericType.setOf(Boolean.class),
        Collections.emptySet()
      },
      {
        DataTypes.setOf(DataTypes.INET),
        GenericType.setOf(InetAddress.class),
        DataTypes.setOf(DataTypes.BOOLEAN),
        GenericType.setOf(Boolean.class),
        Collections.emptySet()
      },
      {
        DataTypes.setOf(tupleType),
        GenericType.setOf(TupleValue.class),
        DataTypes.setOf(DataTypes.BOOLEAN),
        GenericType.setOf(Boolean.class),
        Collections.emptySet()
      },
      {
        DataTypes.setOf(userType),
        GenericType.setOf(UdtValue.class),
        DataTypes.setOf(DataTypes.BOOLEAN),
        GenericType.setOf(Boolean.class),
        Collections.emptySet()
      },
      {
        DataTypes.setOf(DataTypes.setOf(DataTypes.INT)),
        GenericType.setOf(GenericType.setOf(Integer.class)),
        DataTypes.setOf(DataTypes.setOf(DataTypes.BOOLEAN)),
        GenericType.setOf(GenericType.setOf(Boolean.class)),
        ImmutableSet.of(Collections.emptySet())
      },
      {
        DataTypes.setOf(DataTypes.setOf(tupleType)),
        GenericType.setOf(GenericType.setOf(TupleValue.class)),
        DataTypes.setOf(DataTypes.setOf(DataTypes.BOOLEAN)),
        GenericType.setOf(GenericType.setOf(Boolean.class)),
        ImmutableSet.of(Collections.emptySet())
      },
      {
        DataTypes.setOf(DataTypes.setOf(userType)),
        GenericType.setOf(GenericType.setOf(UdtValue.class)),
        DataTypes.setOf(DataTypes.setOf(DataTypes.BOOLEAN)),
        GenericType.setOf(GenericType.setOf(Boolean.class)),
        ImmutableSet.of(Collections.emptySet())
      },
      // maps
      {
        DataTypes.mapOf(DataTypes.INT, DataTypes.TEXT),
        GenericType.mapOf(Integer.class, String.class),
        DataTypes.mapOf(DataTypes.BOOLEAN, DataTypes.BOOLEAN),
        GenericType.mapOf(Boolean.class, Boolean.class),
        Collections.emptyMap()
      },
      {
        DataTypes.mapOf(DataTypes.BLOB, DataTypes.INET),
        GenericType.mapOf(ByteBuffer.class, InetAddress.class),
        DataTypes.mapOf(DataTypes.BOOLEAN, DataTypes.BOOLEAN),
        GenericType.mapOf(Boolean.class, Boolean.class),
        Collections.emptyMap()
      },
      {
        DataTypes.mapOf(tupleType, tupleType),
        GenericType.mapOf(TupleValue.class, TupleValue.class),
        DataTypes.mapOf(DataTypes.BOOLEAN, DataTypes.BOOLEAN),
        GenericType.mapOf(Boolean.class, Boolean.class),
        Collections.emptyMap()
      },
      {
        DataTypes.mapOf(userType, userType),
        GenericType.mapOf(UdtValue.class, UdtValue.class),
        DataTypes.mapOf(DataTypes.BOOLEAN, DataTypes.BOOLEAN),
        GenericType.mapOf(Boolean.class, Boolean.class),
        Collections.emptyMap()
      },
      {
        DataTypes.mapOf(DataTypes.UUID, DataTypes.mapOf(DataTypes.INT, DataTypes.TEXT)),
        GenericType.mapOf(GenericType.UUID, GenericType.mapOf(Integer.class, String.class)),
        DataTypes.mapOf(DataTypes.UUID, DataTypes.mapOf(DataTypes.BOOLEAN, DataTypes.BOOLEAN)),
        GenericType.mapOf(GenericType.UUID, GenericType.mapOf(Boolean.class, Boolean.class)),
        ImmutableMap.of(UUID.randomUUID(), Collections.emptyMap())
      },
      {
        DataTypes.mapOf(DataTypes.mapOf(DataTypes.INT, DataTypes.TEXT), DataTypes.UUID),
        GenericType.mapOf(GenericType.mapOf(Integer.class, String.class), GenericType.UUID),
        DataTypes.mapOf(DataTypes.mapOf(DataTypes.BOOLEAN, DataTypes.BOOLEAN), DataTypes.UUID),
        GenericType.mapOf(GenericType.mapOf(Boolean.class, Boolean.class), GenericType.UUID),
        ImmutableMap.of(Collections.emptyMap(), UUID.randomUUID())
      },
      {
        DataTypes.mapOf(DataTypes.mapOf(userType, userType), DataTypes.mapOf(tupleType, tupleType)),
        GenericType.mapOf(
            GenericType.mapOf(UdtValue.class, UdtValue.class),
            GenericType.mapOf(TupleValue.class, TupleValue.class)),
        DataTypes.mapOf(
            DataTypes.mapOf(DataTypes.BOOLEAN, DataTypes.BOOLEAN),
            DataTypes.mapOf(DataTypes.BOOLEAN, DataTypes.BOOLEAN)),
        GenericType.mapOf(
            GenericType.mapOf(Boolean.class, Boolean.class),
            GenericType.mapOf(Boolean.class, Boolean.class)),
        ImmutableMap.of(Collections.emptyMap(), Collections.emptyMap())
      },
    };
  }

  @DataProvider
  public static Object[][] collectionsWithNullElements() {
    return new Object[][] {
      {
        Collections.singletonList(null),
        "Can't infer list codec because the first element is null "
            + "(note that CQL does not allow null values in collections)"
      },
      {
        Collections.singleton(null),
        "Can't infer set codec because the first element is null "
            + "(note that CQL does not allow null values in collections)"
      },
      {
        Collections.singletonMap("foo", null),
        "Can't infer map codec because the first key and/or value is null "
            + "(note that CQL does not allow null values in collections)"
      },
      {
        Collections.singletonMap(null, "foo"),
        "Can't infer map codec because the first key and/or value is null "
            + "(note that CQL does not allow null values in collections)"
      },
      {
        Collections.singletonMap(null, null),
        "Can't infer map codec because the first key and/or value is null "
            + "(note that CQL does not allow null values in collections)"
      },
    };
  }

  @DataProvider
  public static Object[][] tuplesWithCqlTypes() {
    TupleType tupleType1 = DataTypes.tupleOf(DataTypes.INT, DataTypes.TEXT);
    TupleType tupleType2 = DataTypes.tupleOf(DataTypes.INT, DataTypes.listOf(DataTypes.TEXT));
    TupleType tupleType3 = DataTypes.tupleOf(DataTypes.mapOf(tupleType1, tupleType2));
    TupleValue tupleValue1 = tupleType1.newValue(42, "foo");
    TupleValue tupleValue2 = tupleType2.newValue(42, ImmutableList.of("foo", "bar"));
    return new Object[][] {
      {tupleType1, tupleType1.newValue()},
      {tupleType1, tupleValue1},
      {tupleType2, tupleType2.newValue()},
      {tupleType2, tupleValue2},
      {tupleType3, tupleType3.newValue()},
      {tupleType3, tupleType3.newValue(ImmutableMap.of(tupleValue1, tupleValue2))},
    };
  }

  @DataProvider
  public static Object[][] udtsWithCqlTypes() {
    UserDefinedType userType1 =
        new UserDefinedTypeBuilder(
                CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("type"))
            .withField(CqlIdentifier.fromInternal("field1"), DataTypes.INT)
            .withField(CqlIdentifier.fromInternal("field2"), DataTypes.TEXT)
            .build();
    UserDefinedType userType2 =
        new UserDefinedTypeBuilder(
                CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("type"))
            .withField(CqlIdentifier.fromInternal("field1"), DataTypes.setOf(DataTypes.BIGINT))
            .withField(CqlIdentifier.fromInternal("field2"), DataTypes.listOf(DataTypes.TEXT))
            .build();
    UserDefinedType userType3 =
        new UserDefinedTypeBuilder(
                CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("type"))
            .withField(CqlIdentifier.fromInternal("field1"), DataTypes.mapOf(userType1, userType2))
            .build();
    UdtValue userValue1 = userType1.newValue(42, "foo");
    UdtValue userValue2 =
        userType2.newValue(ImmutableSet.of(24L, 43L), ImmutableList.of("foo", "bar"));
    return new Object[][] {
      {userType1, userType1.newValue()},
      {userType1, userValue1},
      {userType2, userType2.newValue()},
      {userType2, userValue2},
      {userType3, userType3.newValue()},
      {userType3, userType3.newValue(ImmutableMap.of(userValue1, userValue2))},
    };
  }
}
