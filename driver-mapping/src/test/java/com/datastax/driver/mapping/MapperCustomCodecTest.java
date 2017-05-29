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
package com.datastax.driver.mapping;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.utils.CassandraVersion;
import com.datastax.driver.mapping.annotations.*;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@CassandraVersion("2.1.0")
@SuppressWarnings("unused")
public class MapperCustomCodecTest extends CCMTestsSupport {

    @Override
    public void onTestContextInitialized() {
        execute(
                // Columns mapped to custom types
                "CREATE TABLE data1 (i int PRIMARY KEY, l bigint)",
                "INSERT INTO data1 (i, l) VALUES (1, 11)",

                // UDT fields mapped to custom types
                "CREATE TYPE holder(i int, l bigint)",
                "CREATE TABLE data2(i int primary key, data frozen<holder>)",
                "INSERT INTO data2 (i, data) values (1, {i: 1, l: 11})",

                // nested UDT
                // both UDT fields and non-UDT elements in the collection are mapped to custom types
                "CREATE TABLE data3(i int primary key, data map<int, frozen<holder>>)",
                "INSERT INTO data3 (i, data) values (1, {1: {i: 1, l: 11}})"
        );
    }

    @Override
    public Cluster.Builder createClusterBuilder() {
        return Cluster.builder().withCodecRegistry(new CodecRegistry()
                .register(new CustomInt.Codec()));
    }

    @Test(groups = "short")
    public void should_use_custom_codecs_with_basic_operations() {
        Mapper<Data1> mapper = new MappingManager(session()).mapper(Data1.class);

        // Get
        Data1 data11 = mapper.get(new CustomInt(1));
        assertThat(data11.getI()).isEqualTo(new CustomInt(1));
        assertThat(data11.getL()).isEqualTo(new CustomLong(11));

        // Save
        Data1 data12 = new Data1();
        data12.setI(new CustomInt(2));
        data12.setL(new CustomLong(12));
        mapper.save(data12);

        Row row = session().execute("select * from data1 where i = 2").one();
        assertThat(row.getInt(0)).isEqualTo(2);
        assertThat(row.getLong(1)).isEqualTo(12);

        // Delete
        mapper.delete(new CustomInt(2));
        row = session().execute("select * from data1 where i = 2").one();
        assertThat(row).isNull();
    }

    @Test(groups = "short")
    public void should_use_custom_codecs_with_accessors() {
        Data1Accessor accessor = new MappingManager(session()).createAccessor(Data1Accessor.class);
        Data1 data1 = accessor.get(new CustomInt(1));
        assertThat(data1.getI()).isEqualTo(new CustomInt(1));
        assertThat(data1.getL()).isEqualTo(new CustomLong(11));

        accessor.setL(1, new CustomLong(12));
        Row row = session().execute("select l from data1 where i = 1").one();
        assertThat(row.getLong(0)).isEqualTo(12);

        accessor.setL(1, new CustomLong(11));
    }

    @Test(groups = "short")
    public void should_use_custom_codecs_in_UDTs() {
        Mapper<Data2> mapper = new MappingManager(session()).mapper(Data2.class);

        // Get
        Data2 data21 = mapper.get(1);
        assertThat(data21.getData().getI()).isEqualTo(new CustomInt(1));
        assertThat(data21.getData().getL()).isEqualTo(new CustomLong(11));

        // Save
        Data2 data22 = new Data2();
        data22.setI(2);
        data22.setData(new Holder(2, 22));
        mapper.save(data22);

        Row row = session().execute("select * from data2 where i = 2").one();
        assertThat(row.getUDTValue(1).getInt("i")).isEqualTo(2);
        assertThat(row.getUDTValue(1).getLong("l")).isEqualTo(22);

        // cleanup
        mapper.delete(2);
    }

    @Test(groups = "short")
    public void should_use_custom_codecs_in_nested_structures() {
        Mapper<Data3> mapper = new MappingManager(session()).mapper(Data3.class);

        // Get
        Data3 data31 = mapper.get(1);
        assertThat(data31.getData().containsKey(new CustomInt(1)));
        assertThat(data31.getData().get(new CustomInt(1)).getI()).isEqualTo(new CustomInt(1));

        // Save
        Data3 data32 = new Data3();
        data32.setI(2);
        data32.setData(ImmutableMap.of(new CustomInt(2), new Holder(2, 22)));
        mapper.save(data32);

        Row row = session().execute("select * from data3 where i = 2").one();
        Map<Integer, UDTValue> data = row.getMap(1, Integer.class, UDTValue.class);
        assertThat(data.containsKey(2));
        assertThat(data.get(2).getInt("i")).isEqualTo(2);
        assertThat(data.get(2).getLong("l")).isEqualTo(22);

        // cleanup
        mapper.delete(2);
    }

    @Test(groups = "short", expectedExceptions = IllegalArgumentException.class)
    public void should_fail_when_invalid_codec_with_no_default_ctor_provided() {
        new MappingManager(session()).mapper(Data1InvalidCodecNoDefaultConstructor.class);
    }

    @Test(groups = "short", expectedExceptions = InvalidTypeException.class)
    public void should_fail_when_invalid_codec_type_mapping() {
        Mapper<Data1InvalidCodecTypeMapping> mapper = new MappingManager(session()).mapper(Data1InvalidCodecTypeMapping.class);

        // Get should return an InvalidTypeException.
        try {
            mapper.get(1);
            fail("Should not have been able to retrieve.");
        } catch (InvalidTypeException e) {
            // expected.
        }

        // Set should also return an InvalidTypeException.
        Data1InvalidCodecTypeMapping data12 = new Data1InvalidCodecTypeMapping();
        data12.setI(2);
        data12.setL(Optional.of(1L));
        mapper.save(data12);
    }

    @Test(groups = "short", expectedExceptions = IllegalArgumentException.class)
    public void should_fail_when_invalid_codec_with_no_default_ctor_provided_in_accessor() {
        new MappingManager(session()).createAccessor(Data1AccessorNoDefaultConstructor.class);
    }

    @Test(groups = "short", expectedExceptions = InvalidTypeException.class)
    public void should_fail_when_invalid_codec_type_mapping_in_accessor() {
        Data1AccessorInvalidCodecTypeMapping accessor = new MappingManager(session()).createAccessor(Data1AccessorInvalidCodecTypeMapping.class);

        // Get should return an InvalidTypeException.
        try {
            accessor.get(1);
            fail("Should not have been able to retrieve.");
        } catch (InvalidTypeException e) {
            // expected.
        }

        accessor.setL(1, Optional.of(2L));
    }

    @Test(groups = "short")
    public void should_be_able_to_use_parameterized_type() {
        Mapper<Data1ParameterizedType> mapper = new MappingManager(session()).mapper(Data1ParameterizedType.class);

        Data1ParameterizedType data1 = mapper.get(1);
        assertThat(data1.getL()).isEqualTo(Optional.of(11L));

        // Set with an absent ('null') value.
        Data1ParameterizedType empty1 = new Data1ParameterizedType();
        empty1.setI(1000);
        empty1.setL(Optional.<Long>absent());
        mapper.save(empty1);

        // Value should come back absent.
        Data1ParameterizedType empty1r = mapper.get(1000);
        assertThat(empty1r.getL()).isEqualTo(Optional.<Long>absent());

        // Value should come back absent with codec, otherwise null with getObject, 0 with getLong.
        Row row = session().execute("select l from data1 where i=1000").one();
        assertThat(row.getObject(0)).isEqualTo(null); // should map to long codec and return null.
        assertThat(row.getLong(0)).isEqualTo(0L); // default boxed primitive value.
        assertThat(row.get(0, new OptionalOfLong())).isEqualTo(Optional.<Long>absent());

        // Set with a present value.
        Data1ParameterizedType present1 = new Data1ParameterizedType();
        present1.setI(1001);
        present1.setL(Optional.of(20L));
        mapper.save(present1);

        // Value should come back present with codec, otherwise 20L.
        row = session().execute("select l from data1 where i=1001").one();
        assertThat(row.getObject(0)).isEqualTo(20L); // should map to long codec.
        assertThat(row.getLong(0)).isEqualTo(20L);
        assertThat(row.get(0, new OptionalOfLong())).isEqualTo(Optional.of(20L));
    }

    @Table(name = "data1")
    public static class Data1 {
        @PartitionKey
        private CustomInt i;

        @Column(codec = CustomLong.Codec.class)
        private CustomLong l;

        public CustomInt getI() {
            return i;
        }

        public void setI(CustomInt i) {
            this.i = i;
        }

        public CustomLong getL() {
            return l;
        }

        public void setL(CustomLong l) {
            this.l = l;
        }
    }

    @Table(name = "data1")
    public static class Data1InvalidCodecNoDefaultConstructor {

        @PartitionKey
        private int i;

        @Column(codec = NoDefaultConstructorCodec.class)
        private long l;

        public int getI() {
            return i;
        }

        public void setI(int i) {
            this.i = i;
        }

        public long getL() {
            return l;
        }

        public void setL(long l) {
            this.l = l;
        }
    }

    @Table(name = "data1")
    public static class Data1InvalidCodecTypeMapping {

        @PartitionKey
        private int i;

        @Column(codec = OptionalOfString.class)
        private Optional<Long> l;

        public int getI() {
            return i;
        }

        public void setI(int i) {
            this.i = i;
        }

        public Optional<Long> getL() {
            return l;
        }

        public void setL(Optional<Long> l) {
            this.l = l;
        }
    }

    @Table(name = "data1")
    public static class Data1ParameterizedType {

        @PartitionKey
        private int i;

        @Column(codec = OptionalOfLong.class)
        private Optional<Long> l;

        public int getI() {
            return i;
        }

        public void setI(int i) {
            this.i = i;
        }

        public Optional<Long> getL() {
            return l;
        }

        public void setL(Optional<Long> l) {
            this.l = l;
        }
    }

    @Accessor
    public interface Data1Accessor {
        @Query("select * from data1 where i = :i")
        Data1 get(@Param("i") CustomInt i);

        @Query("update data1 set l = :l where i = :i")
        void setL(@Param("i") int i,
                  @Param(value = "l", codec = CustomLong.Codec.class) CustomLong l);
    }

    @Accessor
    public interface Data1AccessorNoDefaultConstructor {

        @Query("update data1 set l = :l where i = :i")
        void setL(@Param("i") int i,
                  @Param(value = "l", codec = NoDefaultConstructorCodec.class) long l);
    }

    @Accessor
    public interface Data1AccessorInvalidCodecTypeMapping {

        @Query("select * from data1 where i = :i")
        Data1InvalidCodecTypeMapping get(@Param("i") int i);

        @Query("update data1 set l = :l where i = :i")
        void setL(@Param("i") int i,
                  @Param(value = "l", codec = OptionalOfString.class) Optional<Long> l);
    }

    @UDT(name = "holder")
    public static class Holder {
        private CustomInt i;

        @Field(codec = CustomLong.Codec.class)
        private CustomLong l;

        public Holder() {
        }

        public Holder(int i, long l) {
            this.i = new CustomInt(i);
            this.l = new CustomLong(l);
        }

        public CustomInt getI() {
            return i;
        }

        public void setI(CustomInt i) {
            this.i = i;
        }

        public CustomLong getL() {
            return l;
        }

        public void setL(CustomLong l) {
            this.l = l;
        }
    }

    @Table(name = "data2")
    public static class Data2 {
        @PartitionKey
        private int i;

        @Frozen
        private Holder data;

        public int getI() {
            return i;
        }

        public void setI(int i) {
            this.i = i;
        }

        public Holder getData() {
            return data;
        }

        public void setData(Holder data) {
            this.data = data;
        }
    }

    @Table(name = "data3")
    public static class Data3 {
        @PartitionKey
        private int i;

        @FrozenValue
        private Map<CustomInt, Holder> data;

        public int getI() {
            return i;
        }

        public void setI(int i) {
            this.i = i;
        }

        public Map<CustomInt, Holder> getData() {
            return data;
        }

        public void setData(Map<CustomInt, Holder> data) {
            this.data = data;
        }
    }

    public static class CustomInt {
        public final int value;

        public CustomInt(int value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof CustomInt) {
                CustomInt that = (CustomInt) other;
                return this.value == that.value;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return value;
        }

        public static class Codec extends TypeCodec<CustomInt> {
            public Codec() {
                super(DataType.cint(), CustomInt.class);
            }

            @Override
            public ByteBuffer serialize(CustomInt value, ProtocolVersion protocolVersion) throws InvalidTypeException {
                return TypeCodec.cint().serialize(value.value, protocolVersion);
            }

            @Override
            public CustomInt deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
                Integer i = TypeCodec.cint().deserialize(bytes, protocolVersion);
                return new CustomInt(i);
            }

            @Override
            public CustomInt parse(String value) throws InvalidTypeException {
                throw new UnsupportedOperationException();
            }

            @Override
            public String format(CustomInt value) throws InvalidTypeException {
                throw new UnsupportedOperationException();
            }
        }
    }

    public static class CustomLong {
        public final long value;

        public CustomLong(long value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof CustomLong) {
                CustomLong that = (CustomLong) other;
                return this.value == that.value;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return (int) (value ^ (value >>> 32));
        }

        public static class Codec extends TypeCodec<CustomLong> {
            public Codec() {
                super(DataType.bigint(), CustomLong.class);
            }

            @Override
            public ByteBuffer serialize(CustomLong value, ProtocolVersion protocolVersion) throws InvalidTypeException {
                return TypeCodec.bigint().serialize(value.value, protocolVersion);
            }

            @Override
            public CustomLong deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
                Long l = TypeCodec.bigint().deserialize(bytes, protocolVersion);
                return new CustomLong(l);
            }

            @Override
            public CustomLong parse(String value) throws InvalidTypeException {
                throw new UnsupportedOperationException();
            }

            @Override
            public String format(CustomLong value) throws InvalidTypeException {
                throw new UnsupportedOperationException();
            }
        }
    }

    public static class OptionalOfLong extends OptionalCodec<Long> {
        private static final CodecRegistry registry = new CodecRegistry();

        public OptionalOfLong() {
            super(registry.codecFor(DataType.bigint(), Long.class));
        }
    }

    public static class OptionalOfString extends OptionalCodec<String> {

        private static final CodecRegistry registry = new CodecRegistry();

        public OptionalOfString() {
            super(registry.codecFor(DataType.text(), String.class));
        }
    }

    /**
     * This class is a copy of GuavaOptionalCodec declared in the extras module,
     * to avoid circular dependencies between Maven modules.
     */
    public static class OptionalCodec<T> extends MappingCodec<Optional<T>, T> {

        private final Predicate<T> isAbsent;

        public OptionalCodec(TypeCodec<T> codec) {
            // @formatter:off
            super(codec,
                    new TypeToken<Optional<T>>() {}.where(new TypeParameter<T>() {}, codec.getJavaType()));
            // @formatter:on
            this.isAbsent = new Predicate<T>() {
                @Override
                public boolean apply(T input) {
                    return input == null
                            || input instanceof Collection && ((Collection) input).isEmpty()
                            || input instanceof Map && ((Map) input).isEmpty();
                }
            };
        }

        @Override
        protected Optional<T> deserialize(T value) {
            return isAbsent(value) ? Optional.<T>absent() : Optional.fromNullable(value);
        }

        @Override
        protected T serialize(Optional<T> value) {
            return value.isPresent() ? value.get() : absentValue();
        }

        protected T absentValue() {
            return null;
        }

        protected boolean isAbsent(T value) {
            return isAbsent.apply(value);
        }

    }

    private static class NoDefaultConstructorCodec extends TypeCodec<String> {

        public NoDefaultConstructorCodec(DataType cqlType, Class<String> javaClass) {
            super(cqlType, javaClass);
        }

        @Override
        public ByteBuffer serialize(String value, ProtocolVersion protocolVersion) throws InvalidTypeException {
            return null;
        }

        @Override
        public String deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
            return null;
        }

        @Override
        public String parse(String value) throws InvalidTypeException {
            return null;
        }

        @Override
        public String format(String value) throws InvalidTypeException {
            return null;
        }

    }

}
