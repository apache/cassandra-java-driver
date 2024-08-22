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
package com.datastax.oss.driver.core.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.data.SettableByIndex;
import com.datastax.oss.driver.api.core.data.SettableByName;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.CustomType;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.type.DefaultListType;
import com.datastax.oss.driver.internal.core.type.DefaultMapType;
import com.datastax.oss.driver.internal.core.type.DefaultSetType;
import com.datastax.oss.driver.internal.core.type.DefaultTupleType;
import com.datastax.oss.driver.internal.core.type.DefaultUserDefinedType;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;

@Category(ParallelizableTests.class)
@RunWith(DataProviderRunner.class)
public class DataTypeIT {
  private static final CcmRule CCM_RULE = CcmRule.getInstance();

  private static final SessionRule<CqlSession> SESSION_RULE = SessionRule.builder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  @Rule public TestName name = new TestName();

  private static final Map<DataType, String> typeToColumnName = new HashMap<>();

  private static final AtomicInteger typeCounter = new AtomicInteger();

  private static final Map<List<DataType>, String> userTypeToTypeName = new HashMap<>();

  private static final AtomicInteger keyCounter = new AtomicInteger();

  private static final String tableName = "data_types_it";

  @DataProvider
  public static Object[][] primitiveTypeSamples() {
    InetAddress address;
    try {
      address = InetAddress.getByAddress(new byte[] {127, 0, 0, 1});
    } catch (UnknownHostException uhae) {
      throw new AssertionError("Could not get address from 127.0.0.1", uhae);
    }

    Object[][] samples =
        new Object[][] {
          new Object[] {DataTypes.ASCII, "ascii"},
          new Object[] {DataTypes.BIGINT, Long.MAX_VALUE},
          new Object[] {DataTypes.BIGINT, null, 0L},
          new Object[] {DataTypes.BLOB, Bytes.fromHexString("0xCAFE")},
          new Object[] {DataTypes.BOOLEAN, Boolean.TRUE},
          new Object[] {DataTypes.BOOLEAN, null, false},
          new Object[] {DataTypes.DECIMAL, new BigDecimal("12.3E+7")},
          new Object[] {DataTypes.DOUBLE, Double.MAX_VALUE},
          new Object[] {DataTypes.DOUBLE, null, 0.0},
          new Object[] {DataTypes.FLOAT, Float.MAX_VALUE},
          new Object[] {DataTypes.FLOAT, null, 0.0f},
          new Object[] {DataTypes.INET, address},
          new Object[] {DataTypes.TINYINT, Byte.MAX_VALUE},
          new Object[] {DataTypes.TINYINT, null, (byte) 0},
          new Object[] {DataTypes.SMALLINT, Short.MAX_VALUE},
          new Object[] {DataTypes.SMALLINT, null, (short) 0},
          new Object[] {DataTypes.INT, Integer.MAX_VALUE},
          new Object[] {DataTypes.INT, null, 0},
          new Object[] {DataTypes.DURATION, CqlDuration.from("PT30H20M")},
          new Object[] {DataTypes.TEXT, "text"},
          new Object[] {DataTypes.TIMESTAMP, Instant.ofEpochMilli(872835240000L)},
          new Object[] {DataTypes.DATE, LocalDate.ofEpochDay(16071)},
          new Object[] {DataTypes.TIME, LocalTime.ofNanoOfDay(54012123450000L)},
          new Object[] {
            DataTypes.TIMEUUID, UUID.fromString("FE2B4360-28C6-11E2-81C1-0800200C9A66")
          },
          new Object[] {DataTypes.UUID, UUID.fromString("067e6162-3b6f-4ae2-a171-2470b63dff00")},
          new Object[] {
            DataTypes.VARINT, new BigInteger(Integer.toString(Integer.MAX_VALUE) + "000")
          }
        };

    Version version = CCM_RULE.getCassandraVersion();
    // Filter types if they aren't supported by cassandra version in use.
    return Arrays.stream(samples)
        .filter(
            o -> {
              DataType dataType = (DataType) o[0];
              if (dataType == DataTypes.DURATION) {
                return version.compareTo(Version.parse("3.10")) >= 0;
              } else if (dataType == DataTypes.TINYINT
                  || dataType == DataTypes.SMALLINT
                  || dataType == DataTypes.DATE
                  || dataType == DataTypes.TIME) {
                return version.compareTo(Version.V2_2_0) >= 0;
              }
              return true;
            })
        .toArray(Object[][]::new);
  }

  @DataProvider
  @SuppressWarnings("unchecked")
  public static Object[][] typeSamples() {
    Object[][] primitiveSamples = primitiveTypeSamples();

    // Build additional data samples from primitive type samples.  For each sample:
    // 1) include the sample itself.
    // 2) include list<type>.
    // 3) include set<type>.
    // 4) include map<int, type>
    // 5) include map<type, int>
    // 6) include tuple<int, type>
    // 7) include udt<int, type>
    return Arrays.stream(primitiveSamples)
        .flatMap(
            o -> {
              List<Object[]> samples = new ArrayList<>();
              samples.add(o);

              if (o[1] == null) {
                // Don't use null values in collections.
                return samples.stream();
              }

              DataType dataType = (DataType) o[0];

              // list of type.
              ListType listType = new DefaultListType((DataType) o[0], false);
              List data = Collections.singletonList(o[1]);
              samples.add(new Object[] {listType, data});

              // set of type.
              if (dataType != DataTypes.DURATION) {
                // durations can't be in sets.
                SetType setType = new DefaultSetType((DataType) o[0], false);
                Set s = Collections.singleton(o[1]);
                samples.add(new Object[] {setType, s});
              }

              // map of int, type.
              MapType mapOfTypeElement = new DefaultMapType(DataTypes.INT, (DataType) o[0], false);
              Map mElement = new HashMap();
              mElement.put(0, o[1]);
              samples.add(new Object[] {mapOfTypeElement, mElement});

              // map of type, int.
              if (dataType != DataTypes.DURATION) {
                // durations can't be map keys.
                MapType mapOfTypeKey = new DefaultMapType((DataType) o[0], DataTypes.INT, false);
                Map mKey = new HashMap();
                mKey.put(o[1], 0);
                samples.add(new Object[] {mapOfTypeKey, mKey});
              }

              // tuple of int, type.
              List<DataType> types = new ArrayList<>();
              types.add(DataTypes.INT);
              types.add(dataType);
              TupleType tupleType = new DefaultTupleType(types);
              TupleValue tupleValue = tupleType.newValue();
              tupleValue = tupleValue.setInt(0, 0);
              setValue(1, tupleValue, dataType, o[1]);
              samples.add(new Object[] {tupleType, tupleValue});

              // tuple of int, type, created using newValue
              TupleValue tupleValue2 = tupleType.newValue(1, o[1]);
              samples.add(new Object[] {tupleType, tupleValue2});

              // udt of int, type.
              final AtomicInteger fieldNameCounter = new AtomicInteger();
              List<CqlIdentifier> typeNames =
                  types.stream()
                      .map(
                          n -> CqlIdentifier.fromCql("field_" + fieldNameCounter.incrementAndGet()))
                      .collect(Collectors.toList());

              UserDefinedType udt =
                  new DefaultUserDefinedType(
                      SESSION_RULE.keyspace(),
                      CqlIdentifier.fromCql(userTypeFor(types)),
                      false,
                      typeNames,
                      types);

              UdtValue udtValue = udt.newValue();
              udtValue = udtValue.setInt(0, 0);
              setValue(1, udtValue, dataType, o[1]);
              samples.add(new Object[] {udt, udtValue});

              // udt of int, type, created using newValue
              UdtValue udtValue2 = udt.newValue(1, o[1]);
              samples.add(new Object[] {udt, udtValue2});

              if (CCM_RULE.getCassandraVersion().compareTo(Version.parse("5.0")) >= 0){
                // vector of type
                CqlVector<?> vector = CqlVector.newInstance(o[1]);
                samples.add(new Object[] {DataTypes.vectorOf(dataType, 1), vector});
              }

              if (o[1].equals("ascii")){
                // once
                CqlVector<?> vector = CqlVector.newInstance(CqlVector.newInstance(1, 2));
                samples.add(new Object[] {DataTypes.vectorOf(DataTypes.vectorOf(DataTypes.INT, 2), 1), vector});
              }
              return samples.stream();
            })
        .toArray(Object[][]::new);
  }

  @BeforeClass
  public static void createTable() {
    // Create a table with all types being tested with.
    // This is a bit more lenient than creating a table for each sample, which would put a lot of
    // burden on C* and
    // the filesystem.
    int counter = 0;

    List<String> columnData = new ArrayList<>();

    for (Object[] sample : typeSamples()) {
      DataType dataType = (DataType) sample[0];

      if (!typeToColumnName.containsKey(dataType)) {
        int columnIndex = ++counter;
        String columnName = "column_" + columnIndex;
        typeToColumnName.put(dataType, columnName);
        columnData.add(String.format("%s %s", columnName, typeFor(dataType)));
      }
    }

    SESSION_RULE
        .session()
        .execute(
            SimpleStatement.builder(
                    String.format(
                        "CREATE TABLE IF NOT EXISTS %s (k int primary key, %s)",
                        tableName, String.join(",", columnData)))
                .setExecutionProfile(SESSION_RULE.slowProfile())
                .build());
  }

  private static String columnNameFor(DataType dataType) {
    return typeToColumnName.get(dataType);
  }

  private static int nextKey() {
    return keyCounter.incrementAndGet();
  }

  @UseDataProvider("typeSamples")
  @Test
  public <K> void should_insert_non_primary_key_column_simple_statement_using_format(
      DataType dataType, K value, K expectedPrimitiveValue) {
    TypeCodec<K> codec = SESSION_RULE.session().getContext().getCodecRegistry().codecFor(dataType);

    int key = nextKey();
    String columnName = columnNameFor(dataType);

    SimpleStatement insert =
        SimpleStatement.builder(
                String.format(
                    "INSERT INTO %s (k, %s) values (?, %s)",
                    tableName, columnName, codec.format(value)))
            .addPositionalValue(key)
            .build();

    SESSION_RULE.session().execute(insert);

    SimpleStatement select =
        SimpleStatement.builder(String.format("SELECT %s FROM %s where k=?", columnName, tableName))
            .addPositionalValue(key)
            .build();

    readValue(select, dataType, value, expectedPrimitiveValue);
  }

  @UseDataProvider("typeSamples")
  @Test
  public <K> void should_insert_non_primary_key_column_simple_statement_positional_value(
      DataType dataType, K value, K expectedPrimitiveValue) {
    int key = nextKey();
    String columnName = columnNameFor(dataType);

    SimpleStatement insert =
        SimpleStatement.builder(
                String.format("INSERT INTO %s (k, %s) values (?, ?)", tableName, columnName))
            .addPositionalValues(key, value)
            .build();

    SESSION_RULE.session().execute(insert);

    SimpleStatement select =
        SimpleStatement.builder(String.format("SELECT %s FROM %s where k=?", columnName, tableName))
            .addPositionalValue(key)
            .build();

    readValue(select, dataType, value, expectedPrimitiveValue);
  }

  @UseDataProvider("typeSamples")
  @Test
  public <K> void should_insert_non_primary_key_column_simple_statement_named_value(
      DataType dataType, K value, K expectedPrimitiveValue) {
    int key = nextKey();
    String columnName = columnNameFor(dataType);

    SimpleStatement insert =
        SimpleStatement.builder(
                String.format("INSERT INTO %s (k, %s) values (:k, :v)", tableName, columnName))
            .addNamedValue("k", key)
            .addNamedValue("v", value)
            .build();

    SESSION_RULE.session().execute(insert);

    SimpleStatement select =
        SimpleStatement.builder(String.format("SELECT %s FROM %s where k=?", columnName, tableName))
            .addNamedValue("k", key)
            .build();

    readValue(select, dataType, value, expectedPrimitiveValue);
  }

  @UseDataProvider("typeSamples")
  @Test
  public <K> void should_insert_non_primary_key_column_bound_statement_positional_value(
      DataType dataType, K value, K expectedPrimitiveValue) {
    int key = nextKey();
    String columnName = columnNameFor(dataType);

    SimpleStatement insert =
        SimpleStatement.builder(
                String.format("INSERT INTO %s (k, %s) values (?, ?)", tableName, columnName))
            .build();

    PreparedStatement preparedInsert = SESSION_RULE.session().prepare(insert);
    BoundStatementBuilder boundBuilder = preparedInsert.boundStatementBuilder();
    boundBuilder = setValue(0, boundBuilder, DataTypes.INT, key);
    boundBuilder = setValue(1, boundBuilder, dataType, value);
    BoundStatement boundInsert = boundBuilder.build();
    SESSION_RULE.session().execute(boundInsert);

    SimpleStatement select =
        SimpleStatement.builder(String.format("SELECT %s FROM %s where k=?", columnName, tableName))
            .build();

    PreparedStatement preparedSelect = SESSION_RULE.session().prepare(select);
    BoundStatement boundSelect = setValue(0, preparedSelect.bind(), DataTypes.INT, key);

    readValue(boundSelect, dataType, value, expectedPrimitiveValue);
  }

  @UseDataProvider("typeSamples")
  @Test
  public <K> void should_insert_non_primary_key_column_bound_statement_named_value(
      DataType dataType, K value, K expectedPrimitiveValue) {
    int key = nextKey();
    String columnName = columnNameFor(dataType);

    SimpleStatement insert =
        SimpleStatement.builder(
                String.format("INSERT INTO %s (k, %s) values (:k, :v)", tableName, columnName))
            .build();

    PreparedStatement preparedInsert = SESSION_RULE.session().prepare(insert);
    BoundStatementBuilder boundBuilder = preparedInsert.boundStatementBuilder();
    boundBuilder = setValue("k", boundBuilder, DataTypes.INT, key);
    boundBuilder = setValue("v", boundBuilder, dataType, value);
    BoundStatement boundInsert = boundBuilder.build();
    SESSION_RULE.session().execute(boundInsert);

    SimpleStatement select =
        SimpleStatement.builder(
                String.format("SELECT %s FROM %s where k=:k", columnName, tableName))
            .build();

    PreparedStatement preparedSelect = SESSION_RULE.session().prepare(select);
    BoundStatement boundSelect = setValue("k", preparedSelect.bind(), DataTypes.INT, key);
    boundSelect = boundSelect.setInt("k", key);

    readValue(boundSelect, dataType, value, expectedPrimitiveValue);
  }

  private static <S extends SettableByIndex<S>> S setValue(
      int index, S bs, DataType dataType, Object value) {
    TypeCodec<Object> codec =
        SESSION_RULE.session() != null
            ? SESSION_RULE.session().getContext().getCodecRegistry().codecFor(dataType)
            : null;

    // set to null if value is null instead of getting possible NPE when casting from null to
    // primitive.
    if (value == null) {
      return bs.setToNull(index);
    }

    switch (dataType.getProtocolCode()) {
      case ProtocolConstants.DataType.ASCII:
      case ProtocolConstants.DataType.VARCHAR:
        bs = bs.setString(index, (String) value);
        break;
      case ProtocolConstants.DataType.BIGINT:
        bs = bs.setLong(index, (long) value);
        break;
      case ProtocolConstants.DataType.BLOB:
        bs = bs.setByteBuffer(index, (ByteBuffer) value);
        break;
      case ProtocolConstants.DataType.BOOLEAN:
        bs = bs.setBoolean(index, (boolean) value);
        break;
      case ProtocolConstants.DataType.DECIMAL:
        bs = bs.setBigDecimal(index, (BigDecimal) value);
        break;
      case ProtocolConstants.DataType.DOUBLE:
        bs = bs.setDouble(index, (double) value);
        break;
      case ProtocolConstants.DataType.FLOAT:
        bs = bs.setFloat(index, (float) value);
        break;
      case ProtocolConstants.DataType.INET:
        bs = bs.setInetAddress(index, (InetAddress) value);
        break;
      case ProtocolConstants.DataType.TINYINT:
        bs = bs.setByte(index, (byte) value);
        break;
      case ProtocolConstants.DataType.SMALLINT:
        bs = bs.setShort(index, (short) value);
        break;
      case ProtocolConstants.DataType.INT:
        bs = bs.setInt(index, (int) value);
        break;
      case ProtocolConstants.DataType.DURATION:
        bs = bs.setCqlDuration(index, (CqlDuration) value);
        break;
      case ProtocolConstants.DataType.TIMESTAMP:
        bs = bs.setInstant(index, (Instant) value);
        break;
      case ProtocolConstants.DataType.DATE:
        bs = bs.setLocalDate(index, (LocalDate) value);
        break;
      case ProtocolConstants.DataType.TIME:
        bs = bs.setLocalTime(index, (LocalTime) value);
        break;
      case ProtocolConstants.DataType.TIMEUUID:
      case ProtocolConstants.DataType.UUID:
        bs = bs.setUuid(index, (UUID) value);
        break;
      case ProtocolConstants.DataType.VARINT:
        bs = bs.setBigInteger(index, (BigInteger) value);
        break;
      case ProtocolConstants.DataType.CUSTOM:
        if (((CustomType) dataType)
            .getClassName()
            .equals("org.apache.cassandra.db.marshal.DurationType")) {
          bs = bs.setCqlDuration(index, (CqlDuration) value);
          break;
        }
        // fall through
      case ProtocolConstants.DataType.LIST:
      case ProtocolConstants.DataType.SET:
      case ProtocolConstants.DataType.MAP:
        bs = bs.set(index, value, codec);
        break;
      case ProtocolConstants.DataType.TUPLE:
        bs = bs.setTupleValue(index, (TupleValue) value);
        break;
      case ProtocolConstants.DataType.UDT:
        bs = bs.setUdtValue(index, (UdtValue) value);
        break;
      default:
        fail("Unhandled DataType " + dataType);
    }
    return bs;
  }

  private static <S extends SettableByName<S>> S setValue(
      String name, S bs, DataType dataType, Object value) {
    TypeCodec<Object> codec =
        SESSION_RULE.session() != null
            ? SESSION_RULE.session().getContext().getCodecRegistry().codecFor(dataType)
            : null;

    // set to null if value is null instead of getting possible NPE when casting from null to
    // primitive.
    if (value == null) {
      return bs.setToNull(name);
    }

    switch (dataType.getProtocolCode()) {
      case ProtocolConstants.DataType.ASCII:
      case ProtocolConstants.DataType.VARCHAR:
        bs = bs.setString(name, (String) value);
        break;
      case ProtocolConstants.DataType.BIGINT:
        bs = bs.setLong(name, (long) value);
        break;
      case ProtocolConstants.DataType.BLOB:
        bs = bs.setByteBuffer(name, (ByteBuffer) value);
        break;
      case ProtocolConstants.DataType.BOOLEAN:
        bs = bs.setBoolean(name, (boolean) value);
        break;
      case ProtocolConstants.DataType.DECIMAL:
        bs = bs.setBigDecimal(name, (BigDecimal) value);
        break;
      case ProtocolConstants.DataType.DOUBLE:
        bs = bs.setDouble(name, (double) value);
        break;
      case ProtocolConstants.DataType.FLOAT:
        bs = bs.setFloat(name, (float) value);
        break;
      case ProtocolConstants.DataType.INET:
        bs = bs.setInetAddress(name, (InetAddress) value);
        break;
      case ProtocolConstants.DataType.TINYINT:
        bs = bs.setByte(name, (byte) value);
        break;
      case ProtocolConstants.DataType.SMALLINT:
        bs = bs.setShort(name, (short) value);
        break;
      case ProtocolConstants.DataType.INT:
        bs = bs.setInt(name, (int) value);
        break;
      case ProtocolConstants.DataType.DURATION:
        bs = bs.setCqlDuration(name, (CqlDuration) value);
        break;
      case ProtocolConstants.DataType.TIMESTAMP:
        bs = bs.setInstant(name, (Instant) value);
        break;
      case ProtocolConstants.DataType.DATE:
        bs = bs.setLocalDate(name, (LocalDate) value);
        break;
      case ProtocolConstants.DataType.TIME:
        bs = bs.setLocalTime(name, (LocalTime) value);
        break;
      case ProtocolConstants.DataType.TIMEUUID:
      case ProtocolConstants.DataType.UUID:
        bs = bs.setUuid(name, (UUID) value);
        break;
      case ProtocolConstants.DataType.VARINT:
        bs = bs.setBigInteger(name, (BigInteger) value);
        break;
      case ProtocolConstants.DataType.CUSTOM:
        if (((CustomType) dataType)
            .getClassName()
            .equals("org.apache.cassandra.db.marshal.DurationType")) {
          bs = bs.setCqlDuration(name, (CqlDuration) value);
          break;
        }
        // fall through
      case ProtocolConstants.DataType.LIST:
      case ProtocolConstants.DataType.SET:
      case ProtocolConstants.DataType.MAP:
        bs = bs.set(name, value, codec);
        break;
      case ProtocolConstants.DataType.TUPLE:
        bs = bs.setTupleValue(name, (TupleValue) value);
        break;
      case ProtocolConstants.DataType.UDT:
        bs = bs.setUdtValue(name, (UdtValue) value);
        break;
      default:
        fail("Unhandled DataType " + dataType);
    }
    return bs;
  }

  private <K> void readValue(
      Statement<?> select, DataType dataType, K value, K expectedPrimitiveValue) {
    TypeCodec<Object> codec =
        SESSION_RULE.session().getContext().getCodecRegistry().codecFor(dataType);
    ResultSet result = SESSION_RULE.session().execute(select);

    String columnName = columnNameFor(dataType);

    List<Row> rows = result.all();
    assertThat(rows).hasSize(1);

    Row row = rows.iterator().next();

    K expectedValue = expectedPrimitiveValue != null ? expectedPrimitiveValue : value;

    switch (dataType.getProtocolCode()) {
      case ProtocolConstants.DataType.ASCII:
      case ProtocolConstants.DataType.VARCHAR:
        assertThat(row.getString(columnName)).isEqualTo(expectedValue);
        assertThat(row.getString(0)).isEqualTo(expectedValue);
        break;
      case ProtocolConstants.DataType.BIGINT:
        assertThat(row.getLong(columnName)).isEqualTo(expectedValue);
        assertThat(row.getLong(0)).isEqualTo(expectedValue);
        break;
      case ProtocolConstants.DataType.BLOB:
        assertThat(row.getByteBuffer(columnName)).isEqualTo(expectedValue);
        assertThat(row.getByteBuffer(0)).isEqualTo(expectedValue);
        break;
      case ProtocolConstants.DataType.BOOLEAN:
        assertThat(row.getBoolean(columnName)).isEqualTo(expectedValue);
        assertThat(row.getBoolean(0)).isEqualTo(expectedValue);
        break;
      case ProtocolConstants.DataType.DECIMAL:
        assertThat(row.getBigDecimal(columnName)).isEqualTo(expectedValue);
        assertThat(row.getBigDecimal(0)).isEqualTo(expectedValue);
        break;
      case ProtocolConstants.DataType.DOUBLE:
        assertThat(row.getDouble(columnName)).isEqualTo(expectedValue);
        assertThat(row.getDouble(0)).isEqualTo(expectedValue);
        break;
      case ProtocolConstants.DataType.FLOAT:
        assertThat(row.getFloat(columnName)).isEqualTo(expectedValue);
        assertThat(row.getFloat(0)).isEqualTo(expectedValue);
        break;
      case ProtocolConstants.DataType.INET:
        assertThat(row.getInetAddress(columnName)).isEqualTo(expectedValue);
        assertThat(row.getInetAddress(0)).isEqualTo(expectedValue);
        break;
      case ProtocolConstants.DataType.TINYINT:
        assertThat(row.getByte(columnName)).isEqualTo(expectedValue);
        assertThat(row.getByte(0)).isEqualTo(expectedValue);
        break;
      case ProtocolConstants.DataType.SMALLINT:
        assertThat(row.getShort(columnName)).isEqualTo(expectedValue);
        assertThat(row.getShort(0)).isEqualTo(expectedValue);
        break;
      case ProtocolConstants.DataType.INT:
        assertThat(row.getInt(columnName)).isEqualTo(expectedValue);
        assertThat(row.getInt(0)).isEqualTo(expectedValue);
        break;
      case ProtocolConstants.DataType.DURATION:
        assertThat(row.getCqlDuration(columnName)).isEqualTo(expectedValue);
        assertThat(row.getCqlDuration(0)).isEqualTo(expectedValue);
        break;
      case ProtocolConstants.DataType.TIMESTAMP:
        assertThat(row.getInstant(columnName)).isEqualTo(expectedValue);
        assertThat(row.getInstant(0)).isEqualTo(expectedValue);
        break;
      case ProtocolConstants.DataType.DATE:
        assertThat(row.getLocalDate(columnName)).isEqualTo(expectedValue);
        assertThat(row.getLocalDate(0)).isEqualTo(expectedValue);
        break;
      case ProtocolConstants.DataType.TIME:
        assertThat(row.getLocalTime(columnName)).isEqualTo(expectedValue);
        assertThat(row.getLocalTime(0)).isEqualTo(expectedValue);
        break;
      case ProtocolConstants.DataType.TIMEUUID:
      case ProtocolConstants.DataType.UUID:
        assertThat(row.getUuid(columnName)).isEqualTo(expectedValue);
        assertThat(row.getUuid(0)).isEqualTo(expectedValue);
        break;
      case ProtocolConstants.DataType.VARINT:
        assertThat(row.getBigInteger(columnName)).isEqualTo(expectedValue);
        assertThat(row.getBigInteger(0)).isEqualTo(expectedValue);
        break;
      case ProtocolConstants.DataType.CUSTOM:
        if (((CustomType) dataType)
            .getClassName()
            .equals("org.apache.cassandra.db.marshal.DurationType")) {
          assertThat(row.getCqlDuration(columnName)).isEqualTo(expectedValue);
          assertThat(row.getCqlDuration(0)).isEqualTo(expectedValue);
          break;
        }
        // fall through
      case ProtocolConstants.DataType.LIST:
      case ProtocolConstants.DataType.MAP:
      case ProtocolConstants.DataType.SET:
        assertThat(row.get(columnName, codec)).isEqualTo(expectedValue);
        assertThat(row.get(0, codec)).isEqualTo(expectedValue);
        break;
      case ProtocolConstants.DataType.TUPLE:
        TupleValue returnedValue = row.getTupleValue(columnName);
        TupleValue exValue = (TupleValue) expectedValue;

        assertThat(returnedValue.getType()).isEqualTo(exValue.getType());
        assertThat(row.getTupleValue(columnName)).isEqualTo(expectedValue);
        assertThat(row.getTupleValue(0)).isEqualTo(expectedValue);
        break;
      case ProtocolConstants.DataType.UDT:
        UdtValue returnedUdtValue = row.getUdtValue(columnName);
        UdtValue exUdtValue = (UdtValue) expectedValue;

        assertThat(returnedUdtValue.getType()).isEqualTo(exUdtValue.getType());
        assertThat(row.getUdtValue(columnName)).isEqualTo(expectedValue);
        assertThat(row.getUdtValue(0)).isEqualTo(expectedValue);
        break;
      default:
        fail("Unhandled DataType " + dataType);
    }

    if (value == null) {
      assertThat(row.isNull(columnName)).isTrue();
      assertThat(row.isNull(0)).isTrue();
    }

    // Decode directly using the codec
    ProtocolVersion protocolVersion = SESSION_RULE.session().getContext().getProtocolVersion();
    assertThat(codec.decode(row.getBytesUnsafe(columnName), protocolVersion)).isEqualTo(value);
    assertThat(codec.decode(row.getBytesUnsafe(0), protocolVersion)).isEqualTo(value);
  }

  private static String typeFor(DataType dataType) {
    String typeName = dataType.asCql(true, true);
    if (dataType instanceof UserDefinedType) {
      UserDefinedType udt = (UserDefinedType) dataType;

      // Create type if it doesn't already exist.
      List<String> fieldParts = new ArrayList<>();
      for (int i = 0; i < udt.getFieldNames().size(); i++) {
        String fieldName = udt.getFieldNames().get(i).asCql(false);
        String fieldType = typeFor(udt.getFieldTypes().get(i));
        fieldParts.add(fieldName + " " + fieldType);
      }

      SESSION_RULE
          .session()
          .execute(
              SimpleStatement.builder(
                      String.format(
                          "CREATE TYPE IF NOT EXISTS %s (%s)",
                          udt.getName().asCql(false), String.join(",", fieldParts)))
                  .setExecutionProfile(SESSION_RULE.slowProfile())
                  .build());

      // Chances are the UDT isn't labeled as frozen in the context we're given, so we add it as
      // older versions of C* don't support non-frozen UDTs.
      if (!udt.isFrozen()) {
        typeName = "frozen<" + typeName + ">";
      }
    }
    return typeName;
  }

  private static String userTypeFor(List<DataType> dataTypes) {
    if (userTypeToTypeName.containsKey(dataTypes)) {
      return userTypeToTypeName.get(dataTypes);
    } else {
      String typeName = "udt_" + typeCounter.incrementAndGet();
      userTypeToTypeName.put(dataTypes, typeName);
      return typeName;
    }
  }
}
