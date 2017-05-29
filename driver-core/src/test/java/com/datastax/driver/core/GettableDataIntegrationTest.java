/*
 * Copyright (C) 2012-2017 DataStax Inc.
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
package com.datastax.driver.core;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.datastax.driver.core.TestUtils.getValue;
import static com.datastax.driver.core.TestUtils.setValue;
import static com.google.common.collect.Lists.newArrayList;
import static org.assertj.core.api.Assertions.assertThat;

public class GettableDataIntegrationTest extends CCMTestsSupport {

    boolean is21;

    CodecRegistry registry = new CodecRegistry();

    // Used for generating unique keys.
    AtomicInteger keyCounter = new AtomicInteger(0);

    @Override
    public void onTestContextInitialized() {
        is21 = ccm().getCassandraVersion().compareTo(VersionNumber.parse("2.1.3")) > 0;
        // only add tuples / nested collections at > 2.1.3.
        execute("CREATE TABLE codec_mapping (k int PRIMARY KEY, "
                + "v int, l list<int>, m map<int,int>" +
                (is21 ? ", t tuple<int,int>, s set<frozen<list<int>>>)" : ")"));
    }

    @Override
    public Cluster.Builder createClusterBuilder() {
        return Cluster.builder().withCodecRegistry(registry);
    }

    @BeforeClass(groups = "short")
    public void setUpRegistry() {
        for (TypeMapping<?> mapping : mappings) {
            registry.register(mapping.codec);
        }
    }

    static final ByteBuffer intBuf = ByteBuffer.allocate(4);

    static {
        intBuf.putInt(1);
        intBuf.flip();
    }

    static InetAddress localhost;

    static {
        try {
            localhost = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            localhost = null;
        }
    }

    // Mappings of Codecs, Data Type to access as (used for determining what get|set method to call), value to set.
    private TypeMapping<?>[] mappings = {
            // set|getString
            new TypeMapping<String>(new IntToStringCodec(), DataType.varchar(), "1"),
            // set|getLong
            new TypeMapping<Long>(new IntToLongCodec(), DataType.bigint(), 1L),
            // set|getBytes
            new TypeMapping<ByteBuffer>(new IntToByteBufferCodec(), DataType.blob(), intBuf),
            // set|getBool
            new TypeMapping<Boolean>(new IntToBooleanCodec(), DataType.cboolean(), true),
            // set|getDecimal
            new TypeMapping<BigDecimal>(new IntToBigDecimalCodec(), DataType.decimal(), new BigDecimal(1)),
            // set|getDouble
            new TypeMapping<Double>(new IntToDoubleCodec(), DataType.cdouble(), 1.0d),
            // set|getFloat
            new TypeMapping<Float>(new IntToFloatCodec(), DataType.cfloat(), 1.0f),
            // set|getInet
            new TypeMapping<InetAddress>(new IntToInetAddressCodec(), DataType.inet(), localhost),
            // set|getTime
            new TypeMapping<Long>(new IntToLongCodec(), DataType.time(), 8675309L),
            // set|getByte
            new TypeMapping<Byte>(new IntToByteCodec(), DataType.tinyint(), (byte) 0xCF),
            // set|getShort
            new TypeMapping<Short>(new IntToShortCodec(), DataType.smallint(), (short) 1003),
            // set|getTimestamp
            new TypeMapping<Date>(new IntToDateCodec(), DataType.timestamp(), new Date(124677)),
            // set|getDate
            new TypeMapping<LocalDate>(new IntToLocalDateCodec(), DataType.date(), LocalDate.fromDaysSinceEpoch(1523)),
            // set|getUUID
            new TypeMapping<UUID>(new IntToUUIDCodec(), DataType.uuid(), new UUID(244242, 0)),
            // set|getVarint
            new TypeMapping<BigInteger>(new IntToBigIntegerCodec(), DataType.varint(), BigInteger.valueOf(4566432L))
    };

    /**
     * Validates that all {@link GettableData} types will allow their get methods to be invoked on a column that does
     * not match data's cql type if a codec is registered that maps the java type of the getter method to the cql type
     * of the column.
     * <p/>
     * Also validates that all {@link SettableData} types will allow their set methods to be invoked on a column that
     * does not match data's cql type if a codec is registered that maps the java type of the setter method to the cql
     * type of the column.
     * <p/>
     * Executes the following for each set|get set:
     * <p/>
     * <ol>
     * <li>Insert row using a prepared statement and binding by name.</li>
     * <li>Insert row using a prepared statement and binding by index.</li>
     * <li>Insert row using a prepared statement and binding everything at once.</li>
     * <li>Retrieve inserted rows and get values by name.</li>
     * <li>Retrieve inserted rows and get values by index.</li>
     * </ol>
     *
     * @jira_ticket JAVA-940
     * @test_category queries
     */
    @Test(groups = "short")
    public void should_allow_getting_and_setting_by_type_if_codec_registered() {
        String insertStmt = "INSERT INTO codec_mapping (k,v,l,m" + (is21 ? ",t,s" : "") + ") values (?,?,?,?" + (is21 ? ",?,?)" : ")");
        PreparedStatement insert = session().prepare(insertStmt);
        PreparedStatement select = session().prepare("SELECT v,l,m" + (is21 ? ",t,s" : "") + " from codec_mapping where k=?");

        TupleType tupleType = new TupleType(newArrayList(DataType.cint(), DataType.cint()),
                cluster().getConfiguration().getProtocolOptions().getProtocolVersion(), registry);

        for (TypeMapping<?> mapping : mappings) {
            // Keys used to insert data in this iteration.
            List<Integer> keys = newArrayList();

            // Values to store.
            Map<Object, Object> map = ImmutableMap.of(mapping.value, mapping.value);
            List<Object> list = newArrayList(mapping.value);
            Set<List<Object>> set = ImmutableSet.of(list);
            TupleValue tupleValue = new TupleValue(tupleType);
            setValue(tupleValue, 0, mapping.outerType, mapping.value);
            setValue(tupleValue, 1, mapping.outerType, mapping.value);

            // Insert by name.
            BoundStatement byName = insert.bind();
            int byNameKey = keyCounter.incrementAndGet();
            keys.add(byNameKey);
            byName.setInt("k", byNameKey);
            setValue(byName, "v", mapping.outerType, mapping.value);
            byName.setList("l", list, mapping.javaType);
            byName.setMap("m", map, mapping.javaType, mapping.javaType);
            if (is21) {
                byName.setTupleValue("t", tupleValue);
                byName.setSet("s", set, TypeTokens.listOf(mapping.javaType));
            }
            session().execute(byName);

            // Insert by index.
            BoundStatement byIndex = insert.bind();
            int byIndexKey = keyCounter.incrementAndGet();
            keys.add(byIndexKey);
            byIndex.setInt(0, byIndexKey);
            setValue(byIndex, 1, mapping.outerType, mapping.value);
            byIndex.setList(2, list, mapping.javaType);
            byIndex.setMap(3, map, mapping.javaType, mapping.javaType);
            if (is21) {
                byIndex.setTupleValue(4, tupleValue);
                byIndex.setSet(5, set, TypeTokens.listOf(mapping.javaType));
            }
            session().execute(byIndex);

            // Insert by binding all at once.
            BoundStatement fullBind;
            int fullBindKey = keyCounter.incrementAndGet();
            keys.add(fullBindKey);
            if (is21) {
                fullBind = insert.bind(fullBindKey, mapping.value, list, map, tupleValue, set);
            } else {
                fullBind = insert.bind(fullBindKey, mapping.value, list, map);
            }
            session().execute(fullBind);

            for (int key : keys) {
                // Retrieve by name.
                Row row = session().execute(select.bind(key)).one();
                assertThat(getValue(row, "v", mapping.outerType, registry)).isEqualTo(mapping.value);
                assertThat(row.getList("l", mapping.codec.getJavaType())).isEqualTo(list);
                assertThat(row.getMap("m", mapping.codec.getJavaType(), mapping.codec.getJavaType())).isEqualTo(map);

                if (is21) {
                    TupleValue returnedTuple = row.getTupleValue("t");
                    assertThat(getValue(returnedTuple, 0, mapping.outerType, registry)).isEqualTo(mapping.value);
                    assertThat(getValue(returnedTuple, 1, mapping.outerType, registry)).isEqualTo(mapping.value);

                    assertThat(row.getSet("s", TypeTokens.listOf(mapping.javaType))).isEqualTo(set);
                }

                // Retrieve by index.
                assertThat(getValue(row, 0, mapping.outerType, registry)).isEqualTo(mapping.value);
                assertThat(row.getList(1, mapping.codec.getJavaType())).isEqualTo(list);
                assertThat(row.getMap(2, mapping.codec.getJavaType(), mapping.codec.getJavaType())).isEqualTo(map);

                if (is21) {
                    TupleValue returnedTuple = row.getTupleValue(3);
                    assertThat(getValue(returnedTuple, 0, mapping.outerType, registry)).isEqualTo(mapping.value);
                    assertThat(getValue(returnedTuple, 1, mapping.outerType, registry)).isEqualTo(mapping.value);

                    assertThat(row.getSet(4, TypeTokens.listOf(mapping.javaType))).isEqualTo(set);
                }
            }
        }
    }

    private static class TypeMapping<T> {
        final TypeCodec<T> codec;
        final TypeToken<Object> javaType;
        final DataType outerType;
        final T value;

        @SuppressWarnings("unchecked")
        TypeMapping(TypeCodec<T> codec, DataType outerType, T value) {
            this.codec = codec;
            this.javaType = (TypeToken<Object>) codec.getJavaType();
            this.outerType = outerType;
            this.value = value;
        }
    }

    // Int <-> Type mappings.
    private static class IntToLongCodec extends MappingCodec<Long, Integer> {

        IntToLongCodec() {
            super(TypeCodec.cint(), Long.class);
        }

        @Override
        protected Long deserialize(Integer value) {
            return value.longValue();
        }

        @Override
        protected Integer serialize(Long value) {
            return value.intValue();
        }
    }

    private static class IntToStringCodec extends MappingCodec<String, Integer> {

        IntToStringCodec() {
            super(TypeCodec.cint(), String.class);
        }

        @Override
        protected String deserialize(Integer value) {
            return value.toString();
        }

        @Override
        protected Integer serialize(String value) {
            return Integer.parseInt(value);
        }
    }

    private static class IntToByteBufferCodec extends MappingCodec<ByteBuffer, Integer> {

        IntToByteBufferCodec() {
            super(TypeCodec.cint(), ByteBuffer.class);
        }

        @Override
        protected ByteBuffer deserialize(Integer value) {
            ByteBuffer buf = ByteBuffer.allocate(4);
            buf.putInt(value);
            buf.flip();
            return buf;
        }

        @Override
        protected Integer serialize(ByteBuffer value) {
            return value.duplicate().getInt();
        }
    }

    private static class IntToBooleanCodec extends MappingCodec<Boolean, Integer> {

        IntToBooleanCodec() {
            super(TypeCodec.cint(), Boolean.class);
        }

        @Override
        protected Boolean deserialize(Integer value) {
            return value != 0;
        }

        @Override
        protected Integer serialize(Boolean value) {
            return value ? 1 : 0;
        }
    }

    private static class IntToBigDecimalCodec extends MappingCodec<BigDecimal, Integer> {

        IntToBigDecimalCodec() {
            super(TypeCodec.cint(), BigDecimal.class);
        }

        @Override
        protected BigDecimal deserialize(Integer value) {
            return new BigDecimal(value);
        }

        @Override
        protected Integer serialize(BigDecimal value) {
            return value.intValue();
        }
    }

    private static class IntToDoubleCodec extends MappingCodec<Double, Integer> {

        IntToDoubleCodec() {
            super(TypeCodec.cint(), Double.class);
        }

        @Override
        protected Double deserialize(Integer value) {
            return value.doubleValue();
        }

        @Override
        protected Integer serialize(Double value) {
            return value.intValue();
        }
    }

    private static class IntToFloatCodec extends MappingCodec<Float, Integer> {

        IntToFloatCodec() {
            super(TypeCodec.cint(), Float.class);
        }

        @Override
        protected Float deserialize(Integer value) {
            return value.floatValue();
        }

        @Override
        protected Integer serialize(Float value) {
            return value.intValue();
        }
    }

    private static class IntToInetAddressCodec extends MappingCodec<InetAddress, Integer> {

        IntToInetAddressCodec() {
            super(TypeCodec.cint(), InetAddress.class);
        }

        @Override
        protected InetAddress deserialize(Integer value) {
            byte[] address = ByteBuffer.allocate(4).putInt(value).array();
            try {
                return InetAddress.getByAddress(address);
            } catch (UnknownHostException e) {
                return null;
            }
        }

        @Override
        protected Integer serialize(InetAddress value) {
            return ByteBuffer.wrap(value.getAddress()).getInt();
        }
    }

    private static class IntToByteCodec extends MappingCodec<Byte, Integer> {

        IntToByteCodec() {
            super(TypeCodec.cint(), Byte.class);
        }

        @Override
        protected Byte deserialize(Integer value) {
            return value.byteValue();
        }

        @Override
        protected Integer serialize(Byte value) {
            return value.intValue();
        }
    }

    private static class IntToShortCodec extends MappingCodec<Short, Integer> {

        IntToShortCodec() {
            super(TypeCodec.cint(), Short.class);
        }

        @Override
        protected Short deserialize(Integer value) {
            return value.shortValue();
        }

        @Override
        protected Integer serialize(Short value) {
            return value.intValue();
        }
    }

    private static class IntToDateCodec extends MappingCodec<Date, Integer> {

        IntToDateCodec() {
            super(TypeCodec.cint(), Date.class);
        }

        @Override
        protected Date deserialize(Integer value) {
            return new Date(value);
        }

        @Override
        protected Integer serialize(Date value) {
            return new Long(value.getTime()).intValue();
        }
    }

    private static class IntToLocalDateCodec extends MappingCodec<LocalDate, Integer> {

        IntToLocalDateCodec() {
            super(TypeCodec.cint(), LocalDate.class);
        }

        @Override
        protected LocalDate deserialize(Integer value) {
            return LocalDate.fromDaysSinceEpoch(value);
        }

        @Override
        protected Integer serialize(LocalDate value) {
            return value.getDaysSinceEpoch();
        }
    }

    private static class IntToUUIDCodec extends MappingCodec<UUID, Integer> {

        IntToUUIDCodec() {
            super(TypeCodec.cint(), UUID.class);
        }

        @Override
        protected UUID deserialize(Integer value) {
            return new UUID(value, 0);
        }

        @Override
        protected Integer serialize(UUID value) {
            return new Long(value.getMostSignificantBits()).intValue();
        }
    }

    private static class IntToBigIntegerCodec extends MappingCodec<BigInteger, Integer> {

        IntToBigIntegerCodec() {
            super(TypeCodec.cint(), BigInteger.class);
        }

        @Override
        protected BigInteger deserialize(Integer value) {
            return BigInteger.valueOf((long) value);
        }

        @Override
        protected Integer serialize(BigInteger value) {
            return value.intValue();
        }
    }
}
