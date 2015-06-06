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
import org.testng.annotations.Test;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.CodecUtils.listOf;
import static com.datastax.driver.core.DataType.*;

public class TypeCodecTest {

    public static final DataType CUSTOM_FOO = DataType.custom("com.example.FooBar");

    public static final TypeCodec.CustomCodec CUSTOM_FOO_CODEC = new TypeCodec.CustomCodec(CUSTOM_FOO);

    private CodecRegistry codecRegistry = CodecRegistry.builder()
        .withDefaultCodecs()
        .withCodec(CUSTOM_FOO_CODEC, true).build();

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
        codec.serialize(list);
    }

    @Test(groups = "unit", expectedExceptions = { IllegalArgumentException.class })
    public void collectionElementTooLargeTest() throws Exception {
        DataType cqlType = DataType.list(DataType.text());
        List<String> list = Lists.newArrayList(Strings.repeat("a", 65536));
        TypeCodec<List<?>> codec = codecRegistry.codecFor(cqlType);
        codec.serialize(list);
    }

    @Test(groups = "unit")
    public void test_cql_text_to_json() {
        JsonCodec<User> codec = new JsonCodec<User>(User.class);
        String json = "{\"id\":1,\"name\":\"John Doe\"}";
        User user = new User(1, "John Doe");
        assertThat(codec.format(user)).isEqualTo(json);
        assertThat(codec.parse(json)).isEqualToComparingFieldByField(user);
        assertThat(codec).canSerialize(user);
    }

    @Test(groups = "unit")
    public void test_cql_list_varchar_to_list_list_integer() {
        ListVarcharToListListInteger codec = new ListVarcharToListListInteger();
        List<List<Integer>> list = new ArrayList<List<Integer>>();
        list.add(Lists.newArrayList(1, 2, 3));
        list.add(Lists.newArrayList(4, 5, 6));
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

    private class ListVarcharToListListInteger extends TypeCodec<List<List<Integer>>> {

        private final ListCodec<String> codec = new ListCodec<String>(VarcharCodec.instance);

        protected ListVarcharToListListInteger() {
            super(list(varchar()), listOf(listOf(Integer.class)));
        }

        @Override
        public ByteBuffer serialize(List<List<Integer>> value) {
            return codec.serialize(Lists.transform(value, new Function<List<Integer>, String>() {
                @Override
                public String apply(List<Integer> input) {
                    return Joiner.on(",").join(input);
                }
            }));
        }

        @Override
        public List<List<Integer>> deserialize(ByteBuffer bytes) {
            return Lists.transform(codec.deserialize(bytes), new Function<String, List<Integer>>() {
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
            return com.google.common.base.Objects.equal(id, user.id) &&
                Objects.equal(name, user.name);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(id, name);
        }
    }
}
