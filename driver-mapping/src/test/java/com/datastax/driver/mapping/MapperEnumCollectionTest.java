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
package com.datastax.driver.mapping;

import com.datastax.driver.core.CCMBridge;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Enumerated;
import com.datastax.driver.mapping.annotations.Frozen;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.datastax.driver.core.Assertions.assertThat;
import static org.junit.Assert.assertNull;

/**
 * Covers handling of collections with enums fields by the mapper.
 */
public class MapperEnumCollectionTest extends CCMBridge.PerClassSingleNodeCluster {

    @Override
    protected Collection<String> getTableDefinitions() {
        return Lists.newArrayList("CREATE TABLE asOrdinal" +
                                    "(k int primary key," +
                                    "v1 List<int>," +
                                    "v2 Set<int>," +
                                    "v3 Map<int, text>," +
                                    "v4 Map<text, int>," +
                                    "v5 List<frozen<Set<int>>>)",
                                "CREATE TABLE asString" +
                                    "(k int primary key," +
                                    "v1 List<text>," +
                                    "v2 Set<text>," +
                                    "v3 Map<text, text>," +
                                    "v4 Map<text, text>," +
                                    "v5 List<frozen<Set<text>>>)");
    }

    public enum Enum {
        FOO, BAR
    }

    @Test(groups = "short")
    public void should_handle_null_enum_as_ordinal() {
        Mapper<AsOrdinal> mapper = new MappingManager(session).mapper(AsOrdinal.class);

        AsOrdinal testTable1 = new AsOrdinal(1);
        testTable1.setV1(null);
        mapper.save(testTable1);
        AsOrdinal o = mapper.get(1);

        assertNull(o.getV1());
    }

    @Test(groups = "short")
    public void should_map_fields_to_collections_as_ordinal() {
        Mapper<AsOrdinal> mapper = new MappingManager(session).mapper(AsOrdinal.class);
        AsOrdinal testTable = new AsOrdinal(1);

        testTable.setV1(ImmutableList.of(Enum.FOO, Enum.BAR));
        testTable.setV2(ImmutableSet.of(Enum.FOO, Enum.BAR));

        Map<Enum, String> v3 = ImmutableMap.<Enum, String>of(Enum.FOO, "bar");
        testTable.setV3(v3);

        Map<String, Enum> v4 = ImmutableMap.<String, Enum>of("foo", Enum.BAR);
        testTable.setV4(v4);

        mapper.save(testTable);

        AsOrdinal testTable2 = mapper.get(1);
        assertThat(testTable2.getV1()).isEqualTo(testTable.getV1()).isNotNull();
        assertThat(testTable2.getV2()).isEqualTo(testTable.getV2()).isNotNull();
        assertThat(testTable2.getV3()).isEqualTo(testTable.getV3()).isNotNull();
        assertThat(testTable2.getV4()).isEqualTo(testTable.getV4()).isNotNull();
    }

    @Test(groups = "short")
    public void should_map_fields_to_nested_collections_as_ordinal() {
        Mapper<AsOrdinal> mapper = new MappingManager(session).mapper(AsOrdinal.class);

        AsOrdinal testTable = new AsOrdinal();
        testTable.setK(1);

        Set<Enum> s1 = Sets.newHashSet(Enum.FOO, Enum.BAR);
        Set<Enum> s2 = Sets.newHashSet(Enum.BAR);
        List<Set<Enum>> ls = new ArrayList<Set<Enum>>();
        ls.add(s1);
        ls.add(s2);
        testTable.setV5(ls);

        mapper.save(testTable);

        AsOrdinal testTable2 = mapper.get(1);
        assertThat(testTable2.getV5()).isEqualTo(testTable.getV5()).isNotNull();
    }


    @Test(groups = "short")
    public void should_handle_null_enum_as_string() {
        Mapper<AsString> mapper = new MappingManager(session).mapper(AsString.class);

        AsString asString = new AsString(1);
        asString.setV1(null);
        mapper.save(asString);

        AsString o = mapper.get(1);

        assertNull(o.getV1());
    }

    @Test(groups = "short")
    public void should_map_fields_to_collections_as_string() {
        Mapper<AsString> mapper = new MappingManager(session).mapper(AsString.class);
        AsString testTable = new AsString(1);

        testTable.setV1(ImmutableList.of(Enum.FOO, Enum.BAR));
        testTable.setV2(ImmutableSet.of(Enum.FOO, Enum.BAR));

        Map<Enum, String> v3 = ImmutableMap.<Enum, String>of(Enum.FOO, "bar");
        testTable.setV3(v3);

        Map<String, Enum> v4 = ImmutableMap.<String, Enum>of("foo", Enum.BAR);
        testTable.setV4(v4);

        mapper.save(testTable);

        AsString testTable2 = mapper.get(1);
        assertThat(testTable2.getV1()).isEqualTo(testTable.getV1()).isNotNull();
        assertThat(testTable2.getV2()).isEqualTo(testTable.getV2()).isNotNull();
        assertThat(testTable2.getV3()).isEqualTo(testTable.getV3()).isNotNull();
        assertThat(testTable2.getV4()).isEqualTo(testTable.getV4()).isNotNull();
    }

    @Test(groups = "short")
    public void should_map_fields_to_nested_collections_as_string() {
        Mapper<AsString> mapper = new MappingManager(session).mapper(AsString.class);

        AsString testTable = new AsString();
        testTable.setK(1);

        Set<Enum> s1 = Sets.newHashSet(Enum.FOO, Enum.BAR);
        Set<Enum> s2 = Sets.newHashSet(Enum.BAR);
        List<Set<Enum>> ls = new ArrayList<Set<Enum>>();
        ls.add(s1);
        ls.add(s2);
        testTable.setV5(ls);

        mapper.save(testTable);

        AsString testTable2 = mapper.get(1);
        assertThat(testTable2.getV5()).isEqualTo(testTable.getV5()).isNotNull();
    }

    @Table(name = "asOrdinal")
    public static class AsOrdinal {
        @PartitionKey
        private int k;

        @Enumerated(EnumType.ORDINAL)
        private List<Enum> v1;

        @Enumerated(EnumType.ORDINAL)
        private Set<Enum> v2;

        @Enumerated(EnumType.ORDINAL)
        private Map<Enum, String> v3;

        @Enumerated(EnumType.ORDINAL)
        private Map<String, Enum> v4;

        @Enumerated(EnumType.ORDINAL)
        @Frozen("List<frozen<set<int>>>")
        private List<Set<Enum>> v5;

        public AsOrdinal() {
        }
        public AsOrdinal(int k) {
            this.k = k;
        }

        public int getK() {
            return k;
        }

        public void setK(int k) {
            this.k = k;
        }

        public List<Enum> getV1() {
            return v1;
        }

        public void setV1(List<Enum> v) {
            this.v1 = v;
        }

        public Set<Enum> getV2() {
            return v2;
        }

        public void setV2(Set<Enum> v) {
            this.v2 = v;
        }

        public Map<Enum, String> getV3() {
            return v3;
        }

        public void setV3(Map<Enum, String> v) {
            this.v3 = v;
        }

        public Map<String, Enum> getV4() {
            return v4;
        }

        public void setV4(Map<String, Enum> v) {
            this.v4 = v;
        }

        public List<Set<Enum>> getV5() {
            return this.v5;
        }

        public void setV5(List<Set<Enum>> v) {
            this.v5 = v;
        }
    }

    @Table(name = "asString")
    public static class AsString {
        @PartitionKey
        private int k;

        @Enumerated(EnumType.STRING)
        private List<Enum> v1;

        @Enumerated(EnumType.STRING)
        private Set<Enum> v2;

        @Enumerated(EnumType.STRING)
        private Map<Enum, String> v3;

        @Enumerated(EnumType.STRING)
        private Map<String, Enum> v4;

        @Enumerated(EnumType.STRING)
        @Frozen("List<frozen<set<text>>>")
        private List<Set<Enum>> v5;

        public AsString(Integer k) {
            this.k = k;
        }

        public AsString() {
        }

        public int getK() {
            return k;
        }

        public void setK(int k) {
            this.k = k;
        }


        public List<Enum> getV1() {
            return v1;
        }

        public void setV1(List<Enum> v) {
            this.v1 = v;
        }

        public Set<Enum> getV2() {
            return v2;
        }

        public void setV2(Set<Enum> v) {
            this.v2 = v;
        }

        public Map<Enum, String> getV3() {
            return v3;
        }

        public void setV3(Map<Enum, String> v) {
            this.v3 = v;
        }

        public Map<String, Enum> getV4() {
            return v4;
        }

        public void setV4(Map<String, Enum> v) {
            this.v4 = v;
        }

        public List<Set<Enum>> getV5() {
            return this.v5;
        }

        public void setV5(List<Set<Enum>> v) {
            this.v5 = v;
        }
    }
}
