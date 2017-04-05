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

import com.datastax.driver.core.UserType.Field;
import com.datastax.driver.core.exceptions.CodecNotFoundException;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.DataType.*;
import static com.datastax.driver.core.ProtocolVersion.V3;
import static com.google.common.collect.Lists.newArrayList;
import static org.testng.Assert.fail;

public class TypeCodecTest {

    public static final DataType CUSTOM_FOO = DataType.custom("com.example.FooBar");

    // @formatter:off
    public static final TypeToken<List<A>> LIST_OF_A_TOKEN = new TypeToken<List<A>>() {};
    public static final TypeToken<List<B>> LIST_OF_B_TOKEN = new TypeToken<List<B>>() {};
    // @formatter:on

    private CodecRegistry codecRegistry = new CodecRegistry();

    @Test(groups = "unit")
    public void testCustomList() throws Exception {
        DataType cqlType = list(CUSTOM_FOO);
        TypeCodec<List<?>> codec = codecRegistry.codecFor(cqlType);
        assertThat(codec).isNotNull().accepts(cqlType);
    }

    @Test(groups = "unit")
    public void testCustomSet() throws Exception {
        DataType cqlType = set(CUSTOM_FOO);
        TypeCodec<Set<?>> codec = codecRegistry.codecFor(cqlType);
        assertThat(codec).isNotNull().accepts(cqlType);
    }

    @Test(groups = "unit")
    public void testCustomKeyMap() throws Exception {
        DataType cqlType = map(CUSTOM_FOO, text());
        TypeCodec<Map<?, ?>> codec = codecRegistry.codecFor(cqlType);
        assertThat(codec).isNotNull().accepts(cqlType);
    }

    @Test(groups = "unit")
    public void testCustomValueMap() throws Exception {
        DataType cqlType = map(text(), CUSTOM_FOO);
        TypeCodec<Map<?, ?>> codec = codecRegistry.codecFor(cqlType);
        assertThat(codec).isNotNull().accepts(cqlType);
    }

    @Test(groups = "unit", expectedExceptions = {IllegalArgumentException.class})
    public void collectionTooLargeTest() throws Exception {
        DataType cqlType = DataType.list(DataType.cint());
        List<Integer> list = Collections.nCopies(65536, 1);
        TypeCodec<List<?>> codec = codecRegistry.codecFor(cqlType);
        codec.serialize(list, ProtocolVersion.V2);
    }

    @Test(groups = "unit", expectedExceptions = {IllegalArgumentException.class})
    public void collectionElementTooLargeTest() throws Exception {
        DataType cqlType = DataType.list(DataType.text());
        List<String> list = newArrayList(Strings.repeat("a", 65536));
        TypeCodec<List<?>> codec = codecRegistry.codecFor(cqlType);
        codec.serialize(list, ProtocolVersion.V2);
    }

    @Test(groups = "unit")
    public void test_cql_list_varchar_to_list_list_integer() {
        ListVarcharToListListInteger codec = new ListVarcharToListListInteger();
        List<List<Integer>> list = new ArrayList<List<Integer>>();
        list.add(newArrayList(1, 2, 3));
        list.add(newArrayList(4, 5, 6));
        assertThat(codec).canSerialize(list);
    }

    @Test(groups = "unit")
    public void test_ascii_vs_utf8() {
        TypeCodec<String> asciiCodec = TypeCodec.ascii();
        TypeCodec<String> utf8Codec = TypeCodec.varchar();
        String ascii = "The quick brown fox jumps over the lazy dog!";
        String utf8 = "Dès Noël, où un zéphyr haï me vêt de glaçons würmiens, je dîne d’exquis rôtis de bœuf au kir à l’aÿ d’âge mûr & cætera!";
        assertThat(asciiCodec)
                .accepts(String.class)
                .accepts(ascii())
                .doesNotAccept(varchar())
                .doesNotAccept(text())
                .accepts(ascii)
                .canSerialize(ascii)
                .cannotSerialize(utf8);
        assertThat(utf8Codec)
                .accepts(String.class)
                .doesNotAccept(ascii())
                .accepts(varchar())
                .accepts(text())
                .accepts(ascii)
                .accepts(utf8)
                .canSerialize(ascii)
                .canSerialize(utf8);
    }

    @Test(groups = "unit")
    public void test_varchar_vs_text() {
        assertThat(TypeCodec.varchar())
                .accepts(String.class)
                .accepts(varchar())
                .accepts(text());
        assertThat(TypeCodec.list(TypeCodec.varchar()))
                .accepts(list(varchar()))
                .accepts(list(text()));
        assertThat(TypeCodec.set(TypeCodec.varchar()))
                .accepts(set(varchar()))
                .accepts(set(text()));
        assertThat(TypeCodec.map(TypeCodec.varchar(), TypeCodec.varchar()))
                .accepts(map(varchar(), varchar()))
                .accepts(map(varchar(), text()))
                .accepts(map(text(), varchar()))
                .accepts(map(text(), text()));
        TupleType t1 = new TupleType(newArrayList(varchar(), varchar()), V3, new CodecRegistry());
        TupleType t2 = new TupleType(newArrayList(text(), varchar()), V3, new CodecRegistry());
        TupleType t3 = new TupleType(newArrayList(varchar(), text()), V3, new CodecRegistry());
        TupleType t4 = new TupleType(newArrayList(text(), text()), V3, new CodecRegistry());
        assertThat(TypeCodec.tuple(t1))
                .accepts(t2)
                .accepts(t3)
                .accepts(t4);
        UserType u1 = new UserType("ks", "table", false, newArrayList(new Field("f1", varchar()), new Field("f2", varchar())), V3, new CodecRegistry());
        UserType u2 = new UserType("ks", "table", false, newArrayList(new Field("f1", text()), new Field("f2", varchar())), V3, new CodecRegistry());
        UserType u3 = new UserType("ks", "table", false, newArrayList(new Field("f1", varchar()), new Field("f2", text())), V3, new CodecRegistry());
        UserType u4 = new UserType("ks", "table", false, newArrayList(new Field("f1", text()), new Field("f2", text())), V3, new CodecRegistry());
        assertThat(TypeCodec.userType(u1))
                .accepts(u2)
                .accepts(u3)
                .accepts(u4);
    }

    @Test(groups = "unit")
    public void test_inheritance() {
        CodecRegistry codecRegistry = new CodecRegistry();
        ACodec aCodec = new ACodec();
        codecRegistry.register(aCodec);
        assertThat(codecRegistry.codecFor(cint(), A.class)).isNotNull().isSameAs(aCodec);
        try {
            // covariance not accepted: no codec handles B exactly
            codecRegistry.codecFor(cint(), B.class);
            fail();
        } catch (CodecNotFoundException e) {
            //ok
        }
        TypeCodec<List<A>> expected = TypeCodec.list(aCodec);
        TypeCodec<List<A>> actual = codecRegistry.codecFor(list(cint()), LIST_OF_A_TOKEN);
        assertThat(actual.getCqlType()).isEqualTo(expected.getCqlType());
        assertThat(actual.getJavaType()).isEqualTo(expected.getJavaType());
        // cannot work: List<B> is not assignable to List<A>
        try {
            codecRegistry.codecFor(list(cint()), LIST_OF_B_TOKEN);
            fail();
        } catch (CodecNotFoundException e) {
            //ok
        }
        codecRegistry = new CodecRegistry();
        BCodec bCodec = new BCodec();
        codecRegistry.register(bCodec);
        try {
            assertThat(codecRegistry.codecFor(cint(), A.class));
            fail();
        } catch (CodecNotFoundException e) {
            // ok
        }
        assertThat(codecRegistry.codecFor(cint(), B.class)).isNotNull().isSameAs(bCodec);
        try {
            codecRegistry.codecFor(list(cint()), LIST_OF_A_TOKEN);
            fail();
        } catch (CodecNotFoundException e) {
            // ok
        }
        TypeCodec<List<B>> expectedB = TypeCodec.list(bCodec);
        TypeCodec<List<B>> actualB = codecRegistry.codecFor(list(cint()), LIST_OF_B_TOKEN);
        assertThat(actualB.getCqlType()).isEqualTo(expectedB.getCqlType());
        assertThat(actualB.getJavaType()).isEqualTo(expectedB.getJavaType());
    }


    @Test(groups = "unit")
    public void should_deserialize_empty_buffer_as_tuple_with_null_values() {
        CodecRegistry codecRegistry = new CodecRegistry();
        TupleType tupleType = new TupleType(newArrayList(DataType.cint(), DataType.varchar(), DataType.cfloat()), ProtocolVersion.NEWEST_SUPPORTED, codecRegistry);
        TupleValue expected = tupleType.newValue(null, null, null);

        TupleValue actual = codecRegistry.codecFor(tupleType, TupleValue.class).deserialize(ByteBuffer.allocate(0), ProtocolVersion.NEWEST_SUPPORTED);
        assertThat(actual).isNotNull();
        assertThat(actual).isEqualTo(expected);
    }

    @Test(groups = "unit")
    public void should_deserialize_empty_buffer_as_udt_with_null_values() {
        CodecRegistry codecRegistry = new CodecRegistry();
        UserType udt = new UserType("ks", "t", false, Arrays.asList(
                new UserType.Field("t", DataType.text()),
                new UserType.Field("i", DataType.cint()),
                new UserType.Field("l", DataType.list(DataType.text()))
        ), ProtocolVersion.NEWEST_SUPPORTED, codecRegistry);
        UDTValue expected = udt.newValue();
        expected.setString("t", null);
        expected.setToNull("i");
        expected.setList("l", null);

        UDTValue actual = codecRegistry.codecFor(udt, UDTValue.class).deserialize(ByteBuffer.allocate(0), ProtocolVersion.NEWEST_SUPPORTED);
        assertThat(actual).isNotNull();
        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Ensures that {@link TypeCodec#timeUUID()} is resolved for all UUIDs and throws an
     * {@link InvalidTypeException} when attempting to serialize or format a non-type 1
     * UUID.
     *
     * @jira_ticket JAVA-965
     */
    @Test(groups = "unit")
    public void should_resolve_timeuuid_codec_for_all_uuids_and_fail_to_serialize_non_type1_uuid() {
        UUID type4UUID = UUID.randomUUID();
        TypeCodec<UUID> codec = codecRegistry.codecFor(DataType.timeuuid(), type4UUID);
        // Should resolve the TimeUUIDCodec, but not serialize/format a type4 uuid with it.
        assertThat(codec).isSameAs(TypeCodec.timeUUID())
                .accepts(UUID.class)
                .cannotSerialize(type4UUID)
                .cannotFormat(type4UUID);
    }


    /**
     * Ensures that primitive types are correctly handled and wrapped when necessary.
     */
    @Test(groups = "unit")
    public void should_wrap_primitive_types() {
        assertThat(TypeCodec.cboolean())
                .accepts(Boolean.class)
                .accepts(Boolean.TYPE)
                .accepts(true);
        assertThat(TypeCodec.cint())
                .accepts(Integer.class)
                .accepts(Integer.TYPE)
                .accepts(42);
        assertThat(TypeCodec.bigint())
                .accepts(Long.class)
                .accepts(Long.TYPE)
                .accepts(42L);
        assertThat(TypeCodec.cfloat())
                .accepts(Float.class)
                .accepts(Float.TYPE)
                .accepts(42.0F);
        assertThat(TypeCodec.cdouble())
                .accepts(Double.class)
                .accepts(Double.TYPE)
                .accepts(42.0D);
    }

    private class ListVarcharToListListInteger extends TypeCodec<List<List<Integer>>> {

        private final TypeCodec<List<String>> codec = TypeCodec.list(TypeCodec.varchar());

        protected ListVarcharToListListInteger() {
            super(DataType.list(DataType.varchar()), TypeTokens.listOf(TypeTokens.listOf(Integer.class)));
        }

        @Override
        public ByteBuffer serialize(List<List<Integer>> value, ProtocolVersion protocolVersion) {
            return codec.serialize(Lists.transform(value, new Function<List<Integer>, String>() {
                @Override
                public String apply(List<Integer> input) {
                    return Joiner.on(",").join(input);
                }
            }), protocolVersion);
        }

        @Override
        public List<List<Integer>> deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
            return Lists.transform(codec.deserialize(bytes, protocolVersion), new Function<String, List<Integer>>() {
                @Override
                public List<Integer> apply(String input) {
                    return Lists.transform(Arrays.asList(input.split(",")), new Function<String, Integer>() {
                        @Override
                        public Integer apply(String input) {
                            return Integer.parseInt(input);
                        }
                    });
                }
            });
        }

        @Override
        public List<List<Integer>> parse(String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String format(List<List<Integer>> value) {
            throw new UnsupportedOperationException();
        }
    }

    class A {

        int i = 0;
    }

    class B extends A {
        {
            i = 1;
        }
    }

    class ACodec extends TypeCodec<A> {

        protected ACodec() {
            super(DataType.cint(), A.class);
        }

        @Override
        public ByteBuffer serialize(A value, ProtocolVersion protocolVersion) throws InvalidTypeException {
            return null; // not tested
        }

        @Override
        public A deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
            return null; // not tested
        }

        @Override
        public A parse(String value) throws InvalidTypeException {
            return null; // not tested
        }

        @Override
        public String format(A value) throws InvalidTypeException {
            return null; // not tested
        }
    }


    class BCodec extends TypeCodec<B> {

        protected BCodec() {
            super(DataType.cint(), B.class);
        }

        @Override
        public ByteBuffer serialize(B value, ProtocolVersion protocolVersion) throws InvalidTypeException {
            return null; // not tested
        }

        @Override
        public B deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
            return null; // not tested
        }

        @Override
        public B parse(String value) throws InvalidTypeException {
            return null; // not tested
        }

        @Override
        public String format(B value) throws InvalidTypeException {
            return null; // not tested
        }
    }
}
