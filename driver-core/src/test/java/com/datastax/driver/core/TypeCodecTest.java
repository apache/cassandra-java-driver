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

import java.nio.ByteBuffer;
import java.util.*;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import org.testng.annotations.Test;

import static com.google.common.collect.Lists.newArrayList;
import static org.testng.Assert.fail;

import com.datastax.driver.core.TypeCodec.UDTCodec;
import com.datastax.driver.core.UserType.Field;
import com.datastax.driver.core.exceptions.CodecNotFoundException;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.CodecUtils.listOf;
import static com.datastax.driver.core.DataType.*;
import static com.datastax.driver.core.ProtocolVersion.V3;

public class TypeCodecTest {

    public static final DataType CUSTOM_FOO = DataType.custom("com.example.FooBar");

    public static final TypeCodec.CustomCodec CUSTOM_FOO_CODEC = new TypeCodec.CustomCodec(CUSTOM_FOO);

    private CodecRegistry codecRegistry = new CodecRegistry()
        .register(CUSTOM_FOO_CODEC);

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

    @Test(groups = "unit", expectedExceptions = { IllegalArgumentException.class })
    public void collectionTooLargeTest() throws Exception {
        DataType cqlType = DataType.list(DataType.cint());
        List<Integer> list = Collections.nCopies(65536, 1);
        TypeCodec<List<?>> codec = codecRegistry.codecFor(cqlType);
        codec.serialize(list, ProtocolVersion.V2);
    }

    @Test(groups = "unit", expectedExceptions = { IllegalArgumentException.class })
    public void collectionElementTooLargeTest() throws Exception {
        DataType cqlType = DataType.list(DataType.text());
        List<String> list = newArrayList(Strings.repeat("a", 65536));
        TypeCodec<List<?>> codec = codecRegistry.codecFor(cqlType);
        codec.serialize(list, ProtocolVersion.V2);
    }

    @Test(groups = "unit")
    public void test_cql_text_to_json() {
        JsonCodec<User> codec = new JsonCodec<User>(User.class);
        // the codec is expected to format json objects as json strings enclosed in single quotes,
        // as it is required for CQL literals of varchar type.
        String json = "'{\"id\":1,\"name\":\"John Doe\"}'";
        User user = new User(1, "John Doe");
        assertThat(codec.format(user)).isEqualTo(json);
        assertThat(codec.parse(json)).isEqualToComparingFieldByField(user);
        assertThat(codec).canSerialize(user);
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
        TypeCodec.AsciiCodec asciiCodec = TypeCodec.AsciiCodec.instance;
        TypeCodec.VarcharCodec utf8Codec = TypeCodec.VarcharCodec.instance;
        String ascii = "The quick brown fox jumps over the lazy dog!";
        String utf8 = "Dès Noël, où un zéphyr haï me vêt de glaçons würmiens, je dîne d’exquis rôtis de bœuf au kir à l’aÿ d’âge mûr & cætera!";
        assertThat(asciiCodec)
            .accepts(String.class)
            .accepts(ascii())
            .doesNotAccept(varchar())
            .doesNotAccept(text())
            .accepts(ascii)
            .doesNotAccept(utf8)
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
        assertThat(TypeCodec.VarcharCodec.instance)
            .accepts(String.class)
            .accepts(varchar())
            .accepts(text());
        assertThat(new TypeCodec.ListCodec<String>(TypeCodec.VarcharCodec.instance))
            .accepts(list(varchar()))
            .accepts(list(text()));
        assertThat(new TypeCodec.SetCodec<String>(TypeCodec.VarcharCodec.instance))
            .accepts(set(varchar()))
            .accepts(set(text()));
        assertThat(new TypeCodec.MapCodec<String, String>(TypeCodec.VarcharCodec.instance, TypeCodec.VarcharCodec.instance))
            .accepts(map(varchar(), varchar()))
            .accepts(map(varchar(), text()))
            .accepts(map(text(), varchar()))
            .accepts(map(text(), text()));
        TupleType t1 = new TupleType(newArrayList(varchar(), varchar()), V3, new CodecRegistry());
        TupleType t2 = new TupleType(newArrayList(text(), varchar()), V3, new CodecRegistry());
        TupleType t3 = new TupleType(newArrayList(varchar(), text()), V3, new CodecRegistry());
        TupleType t4 = new TupleType(newArrayList(text(), text()), V3, new CodecRegistry());
        assertThat(new TypeCodec.TupleCodec(t1))
            .accepts(t2)
            .accepts(t3)
            .accepts(t4);
        UserType u1 = new UserType("ks", "table", newArrayList(new Field("f1", varchar()), new Field("f2", varchar())), V3, new CodecRegistry());
        UserType u2 = new UserType("ks", "table", newArrayList(new Field("f1", text()), new Field("f2", varchar())), V3, new CodecRegistry());
        UserType u3 = new UserType("ks", "table", newArrayList(new Field("f1", varchar()), new Field("f2", text())), V3, new CodecRegistry());
        UserType u4 = new UserType("ks", "table", newArrayList(new Field("f1", text()), new Field("f2", text())), V3, new CodecRegistry());
        assertThat(new UDTCodec(u1))
            .accepts(u2)
            .accepts(u3)
            .accepts(u4);
    }

    @Test(groups = "unit")
    public void test_enum() {
        EnumStringCodec<FooBarQix> codec = new EnumStringCodec<FooBarQix>(FooBarQix.class);
        assertThat(codec)
            .canSerialize(FooBarQix.FOO);
    }

    @Test(groups = "unit")
    public void test_inheritance() {
        CodecRegistry codecRegistry = new CodecRegistry();
        ACodec aCodec = new ACodec();
        codecRegistry.register(aCodec);
        assertThat(codecRegistry.codecFor(cint(), A.class)).isNotNull().isSameAs(aCodec);
        // inheritance works: B is assignable to A
        assertThat(codecRegistry.codecFor(cint(), B.class)).isNotNull().isSameAs(aCodec);
        TypeCodec.ListCodec<A> expected = new TypeCodec.ListCodec<A>(aCodec);
        TypeCodec<List<A>> actual = codecRegistry.codecFor(list(cint()), new TypeToken<List<A>>(){});
        assertThat(actual.getCqlType()).isEqualTo(expected.getCqlType());
        assertThat(actual.getJavaType()).isEqualTo(expected.getJavaType());
        // cannot work: List<B> is not assignable to List<A>
        try {
            codecRegistry.codecFor(list(cint()), new TypeToken<List<B>>(){});
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
            codecRegistry.codecFor(list(cint()), new TypeToken<List<A>>(){});
            fail();
        } catch (CodecNotFoundException e) {
            // ok
        }
        TypeCodec<List<B>> actualB = codecRegistry.codecFor(list(cint()), new TypeToken<List<B>>(){});
        TypeCodec.ListCodec<B> expectedB = new TypeCodec.ListCodec<B>(bCodec);
        assertThat(actualB.getCqlType()).isEqualTo(expectedB.getCqlType());
        assertThat(actualB.getJavaType()).isEqualTo(expectedB.getJavaType());
    }


    @Test(groups = "unit")
    public void should_deserialize_empty_buffer_as_tuple_with_null_values() {
        CodecRegistry codecRegistry = CodecRegistry.DEFAULT_INSTANCE;
        TupleType tupleType = new TupleType(newArrayList(DataType.cint(), DataType.varchar(), DataType.cfloat()), ProtocolVersion.NEWEST_SUPPORTED, codecRegistry);
        TupleValue expected = tupleType.newValue(null, null, null);

        TupleValue actual = codecRegistry.codecFor(tupleType, TupleValue.class).deserialize(ByteBuffer.allocate(0), ProtocolVersion.NEWEST_SUPPORTED);
        assertThat(actual).isNotNull();
        assertThat(actual).isEqualTo(expected);
    }

    @Test(groups = "unit")
    public void should_deserialize_empty_buffer_as_udt_with_null_values() {
        CodecRegistry codecRegistry = CodecRegistry.DEFAULT_INSTANCE;
        UserType udt = new UserType("ks", "t", Arrays.asList(
            new UserType.Field("t", DataType.text()),
            new UserType.Field("i", DataType.cint()),
            new UserType.Field("l", DataType.list(DataType.text()))
        ), ProtocolVersion.NEWEST_SUPPORTED, codecRegistry);
        UDTValue expected = udt.newValue();
        expected.setString("t", null);
        expected.setObject("i", null);
        expected.setList("l", null);

        UDTValue actual = codecRegistry.codecFor(udt, UDTValue.class).deserialize(ByteBuffer.allocate(0), ProtocolVersion.NEWEST_SUPPORTED);
        assertThat(actual).isNotNull();
        assertThat(actual).isEqualTo(expected);
    }


    private class ListVarcharToListListInteger extends TypeCodec<List<List<Integer>>> {

        private final ListCodec<String> codec = new ListCodec<String>(VarcharCodec.instance);

        protected ListVarcharToListListInteger() {
            super(list(varchar()), listOf(listOf(Integer.class)));
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

    @SuppressWarnings("unused")
    public static class User {

        private int id;

        private String name;

        @JsonCreator
        public User(@JsonProperty("id") int id, @JsonProperty("name") String name) {
            this.id = id;
            this.name = name;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            User user = (User)o;
            return Objects.equal(id, user.id) &&
                Objects.equal(name, user.name);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(id, name);
        }
    }

    enum FooBarQix {
        FOO, BAR, QIX
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
            super(cint(), A.class);
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
            super(cint(), B.class);
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
