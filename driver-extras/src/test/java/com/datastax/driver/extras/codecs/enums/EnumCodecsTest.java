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
package com.datastax.driver.extras.codecs.enums;

import com.datastax.driver.core.*;
import com.datastax.driver.core.utils.CassandraVersion;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.annotations.*;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.datastax.driver.core.DataType.cint;
import static com.datastax.driver.core.DataType.text;
import static com.datastax.driver.extras.codecs.enums.EnumCodecsTest.Bar.BAR_1;
import static com.datastax.driver.extras.codecs.enums.EnumCodecsTest.Bar.BAR_2;
import static com.datastax.driver.extras.codecs.enums.EnumCodecsTest.Foo.FOO_1;
import static com.datastax.driver.extras.codecs.enums.EnumCodecsTest.Foo.FOO_2;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A test that validates that Enums are correctly mapped to varchars (with EnumNameCodec)
 * and ints (with EnumOrdinalCodec).
 * It also validates that both codecs may coexist in the same CodecRegistry.
 */
@CassandraVersion("2.1.0")
public class EnumCodecsTest extends CCMTestsSupport {

    private final String insertQuery = "INSERT INTO t1 (pk, foo, foos, bar, bars, foobars, tup, udt) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
    private final String selectQuery = "SELECT pk, foo, foos, bar, bars, foobars, tup, udt FROM t1 WHERE pk = ?";

    private final int pk = 42;

    private final List<Foo> foos = newArrayList(FOO_1, FOO_2);
    private final Set<Bar> bars = newHashSet(BAR_1, BAR_2);
    private final Map<Foo, Bar> foobars = ImmutableMap.of(FOO_1, BAR_1, FOO_2, BAR_2);

    private TupleValue tupleValue;
    private UDTValue udtValue;

    @Override
    public void onTestContextInitialized() {
        execute(
                "CREATE TYPE IF NOT EXISTS udt1 ("
                        + "foo int,"
                        + "bar text)",
                "CREATE TABLE IF NOT EXISTS t1 ("
                        + "pk int, "
                        + "foo int, "
                        + "foos list<int>, "
                        + "bar text, "
                        + "bars set<text>, "
                        + "foobars map<int,text>, "
                        + "tup frozen<tuple<int,varchar>>, "
                        + "udt frozen<udt1>,"
                        + "primary key (pk, foo))"
        );
    }

    @Override
    public Cluster.Builder createClusterBuilder() {
        return Cluster.builder().withCodecRegistry(
                new CodecRegistry()
                        .register(new EnumOrdinalCodec<Foo>(Foo.class))
                        .register(new EnumNameCodec<Bar>(Bar.class))
        );
    }

    @BeforeMethod(groups = "short")
    public void before() {
        TupleType tup = cluster().getMetadata().newTupleType(cint(), text());
        tupleValue = tup.newValue()
                .set(0, FOO_1, Foo.class)
                .set(1, BAR_1, Bar.class);
        UserType udt = cluster().getMetadata().getKeyspace(keyspace).getUserType("udt1");
        udtValue = udt.newValue()
                .set("foo", FOO_1, Foo.class)
                .set("bar", BAR_1, Bar.class);
    }

    @Test(groups = "short")
    public void should_use_enum_codecs_with_simple_statements() {
        session().execute(insertQuery, pk, FOO_1, foos, BAR_1, bars, foobars, tupleValue, udtValue);
        ResultSet rows = session().execute(selectQuery, pk);
        Row row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_use_enum_codecs_with_prepared_statements_1() {
        session().execute(session().prepare(insertQuery).bind(pk, FOO_1, foos, BAR_1, bars, foobars, tupleValue, udtValue));
        PreparedStatement ps = session().prepare(selectQuery);
        ResultSet rows = session().execute(ps.bind(pk));
        Row row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_use_enum_codecs_with_prepared_statements_2() {
        session().execute(session().prepare(insertQuery).bind()
                        .setInt(0, pk)
                        .set(1, FOO_1, Foo.class)
                        .setList(2, foos, Foo.class)
                        .set(3, BAR_1, Bar.class)
                        .set(4, bars, TypeTokens.setOf(Bar.class))
                        .setMap(5, foobars, Foo.class, Bar.class)
                        .setTupleValue(6, tupleValue)
                        .setUDTValue(7, udtValue)
        );
        PreparedStatement ps = session().prepare(selectQuery);
        ResultSet rows = session().execute(ps.bind()
                        .setInt(0, pk)
        );
        Row row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_use_mapper_to_store_and_retrieve_nulls_with_enum_codecs() {
        // given
        MappingManager manager = new MappingManager(session());
        Mapper<Mapped> mapper = manager.mapper(Mapped.class);
        Mapped pojo = new Mapped();
        pojo.pk = 42;
        pojo.foo = FOO_1;
        // when
        mapper.save(pojo);
        Mapped actual = mapper.get(42, FOO_1);
        // then
        assertThat(actual).isEqualToComparingFieldByField(pojo);
    }

    @Test(groups = "short")
    public void should_use_mapper_to_store_and_retrieve_values_with_enum_codecs() {
        // given
        MappingManager manager = new MappingManager(session());
        Mapper<Mapped> mapper = manager.mapper(Mapped.class);
        Mapped pojo = new Mapped();
        pojo.pk = 42;
        pojo.foo = FOO_1;
        pojo.bar = BAR_1;
        pojo.foos = newArrayList(FOO_2, FOO_1);
        pojo.bars = newHashSet(BAR_1, BAR_2);
        pojo.foobars = ImmutableMap.of(FOO_1, BAR_2);
        // when
        mapper.save(pojo);
        Mapped actual = mapper.get(42, FOO_1);
        // then
        assertThat(actual).isEqualToComparingFieldByField(pojo);
    }

    @Test(groups = "short")
    public void should_use_accessor_to_store_and_retrieve_values_with_enum_codecs() {
        // given
        MappedAccessor accessor = new MappingManager(session()).createAccessor(MappedAccessor.class);
        Mapped expected = new Mapped();
        expected.pk = 42;
        expected.foo = FOO_1;
        expected.bar = BAR_2;
        // when
        accessor.insert(42, FOO_1, BAR_2);
        Mapped result = accessor.getByFoo(42, FOO_1);
        // then
        assertThat(result).isEqualToComparingFieldByField(expected);
    }

    private void assertRow(Row row) {

        assertThat(row.getInt(0)).isEqualTo(pk);

        assertThat(row.getObject(1)).isEqualTo(FOO_1.ordinal()); // uses the built-in IntCodec because CQL type is int
        assertThat(row.getInt("foo")).isEqualTo(FOO_1.ordinal()); // uses the built-in IntCodec
        assertThat(row.get(1, Integer.class)).isEqualTo(FOO_1.ordinal()); // forces IntCodec
        assertThat(row.get("foo", Foo.class)).isEqualTo(FOO_1);  // forces EnumOrdinalCodec

        assertThat(row.getObject(2)).isEqualTo(newArrayList(FOO_1.ordinal(), FOO_2.ordinal())); // uses the built-in ListCodec(IntCodec) because CQL type is list<int>
        assertThat(row.getList(2, Integer.class)).isEqualTo(newArrayList(FOO_1.ordinal(), FOO_2.ordinal()));
        assertThat(row.getList("foos", Foo.class)).isEqualTo(newArrayList(FOO_1, FOO_2));
        assertThat(row.get(2, TypeTokens.listOf(Integer.class))).isEqualTo(newArrayList(FOO_1.ordinal(), FOO_2.ordinal()));
        assertThat(row.get("foos", TypeTokens.listOf(Foo.class))).isEqualTo(newArrayList(FOO_1, FOO_2));

        assertThat(row.getObject(3)).isEqualTo(BAR_1.name()); // uses the built-in VarcharCodec because CQL type is varchar
        assertThat(row.getString("bar")).isEqualTo(BAR_1.name()); // forces VarcharCodec
        assertThat(row.get(3, String.class)).isEqualTo(BAR_1.name()); // forces VarcharCodec
        assertThat(row.get("bar", Bar.class)).isEqualTo(BAR_1); // forces EnumNameCodec

        assertThat(row.getObject(4)).isEqualTo(newHashSet(BAR_1.name(), BAR_2.name())); // uses the built-in SetCodec(VarcharCodec) because CQL type is set<varchar>
        assertThat(row.getSet(4, String.class)).isEqualTo(newHashSet(BAR_1.name(), BAR_2.name()));
        assertThat(row.getSet("bars", Bar.class)).isEqualTo(newHashSet(BAR_1, BAR_2));
        assertThat(row.get(4, TypeTokens.setOf(String.class))).isEqualTo(newHashSet(BAR_1.name(), BAR_2.name()));
        assertThat(row.get("bars", TypeTokens.setOf(Bar.class))).isEqualTo(newHashSet(BAR_1, BAR_2));

        // uses default built-in codec MapCodec(IntCodec, VarcharCodec) because CQL type is map<int, varchar>
        assertThat(row.getObject(5)).isEqualTo(ImmutableMap.of(FOO_1.ordinal(), BAR_1.name(), FOO_2.ordinal(), BAR_2.name()));

        // getMap with combinations of built-in or EnumX codecs
        assertThat(row.getMap(5, Integer.class, String.class)).isEqualTo(ImmutableMap.of(FOO_1.ordinal(), BAR_1.name(), FOO_2.ordinal(), BAR_2.name()));
        assertThat(row.getMap(5, Foo.class, String.class)).isEqualTo(ImmutableMap.of(FOO_1, BAR_1.name(), FOO_2, BAR_2.name()));
        assertThat(row.getMap(5, Integer.class, Bar.class)).isEqualTo(ImmutableMap.of(FOO_1.ordinal(), BAR_1, FOO_2.ordinal(), BAR_2));
        assertThat(row.getMap(5, Foo.class, Bar.class)).isEqualTo(ImmutableMap.of(FOO_1, BAR_1, FOO_2, BAR_2));

        // get + TypeToken with combinations of built-in or EnumX codecs
        assertThat(row.get("foobars", TypeTokens.mapOf(Integer.class, String.class))).isEqualTo(ImmutableMap.of(FOO_1.ordinal(), BAR_1.name(), FOO_2.ordinal(), BAR_2.name()));
        assertThat(row.get("foobars", TypeTokens.mapOf(Foo.class, String.class))).isEqualTo(ImmutableMap.of(FOO_1, BAR_1.name(), FOO_2, BAR_2.name()));
        assertThat(row.get("foobars", TypeTokens.mapOf(Integer.class, Bar.class))).isEqualTo(ImmutableMap.of(FOO_1.ordinal(), BAR_1, FOO_2.ordinal(), BAR_2));
        assertThat(row.get("foobars", TypeTokens.mapOf(Foo.class, Bar.class))).isEqualTo(ImmutableMap.of(FOO_1, BAR_1, FOO_2, BAR_2));

        assertThat(row.getTupleValue(6).getInt(0)).isEqualTo(FOO_1.ordinal());
        assertThat(row.get(6, TupleValue.class).get(0, Foo.class)).isEqualTo(FOO_1);
        assertThat(row.getTupleValue("tup").getString(1)).isEqualTo(BAR_1.name());
        assertThat(row.get("tup", TupleValue.class).get(1, Bar.class)).isEqualTo(BAR_1);

        assertThat(row.getUDTValue(7).getInt("foo")).isEqualTo(FOO_1.ordinal());
        assertThat(row.get(7, UDTValue.class).get("foo", Foo.class)).isEqualTo(FOO_1);
        assertThat(row.getUDTValue("udt").getString("bar")).isEqualTo(BAR_1.name());
        assertThat(row.get("udt", UDTValue.class).get("bar", Bar.class)).isEqualTo(BAR_1);

    }

    @SuppressWarnings("unused")
    @Table(name = "t1")
    public static class Mapped {

        @PartitionKey
        private int pk;

        @ClusteringColumn
        private Foo foo;

        private List<Foo> foos;

        private Bar bar;

        private Set<Bar> bars;

        private Map<Foo, Bar> foobars;

        public int getPk() {
            return pk;
        }

        public void setPk(int pk) {
            this.pk = pk;
        }

        public Foo getFoo() {
            return foo;
        }

        public void setFoo(Foo foo) {
            this.foo = foo;
        }

        public List<Foo> getFoos() {
            return foos;
        }

        public void setFoos(List<Foo> foos) {
            this.foos = foos;
        }

        public Bar getBar() {
            return bar;
        }

        public void setBar(Bar bar) {
            this.bar = bar;
        }

        public Set<Bar> getBars() {
            return bars;
        }

        public void setBars(Set<Bar> bars) {
            this.bars = bars;
        }

        public Map<Foo, Bar> getFoobars() {
            return foobars;
        }

        public void setFoobars(Map<Foo, Bar> foobars) {
            this.foobars = foobars;
        }
    }

    @Accessor
    public interface MappedAccessor {

        @Query("select * from t1 where pk=? and foo=?")
        Mapped getByFoo(int pk, Foo foo);

        @Query("insert into t1 (pk, foo, bar) values (?,?,?)")
        ResultSet insert(int pk, Foo foo, Bar bar);

    }


    enum Foo {
        FOO_1, FOO_2
    }

    enum Bar {
        BAR_1, BAR_2
    }

}
