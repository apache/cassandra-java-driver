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

import com.datastax.driver.core.CCMTestsSupport;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.utils.CassandraVersion;
import com.datastax.driver.mapping.annotations.*;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.datastax.driver.core.Assertions.assertThat;

@CassandraVersion("2.1.3")
@SuppressWarnings("unused")
public class MapperNestedCollectionsTest extends CCMTestsSupport {

    @Override
    public void onTestContextInitialized() {
        execute(
                "CREATE TYPE testType(i int, ls list<frozen<set<int>>>)",
                "CREATE TABLE testTable (k int primary key, "
                        + "m1 map<text, frozen<map<int, int>>>, " // nested collection
                        + "m2 map<text, frozen<map<int, frozen<testType>>>>)" // nested collection with UDT
        );
    }

    @Test(groups = "short")
    public void should_map_fields_to_nested_collections() {
        Mapper<TestTable> mapper = new MappingManager(session()).mapper(TestTable.class);

        TestTable testTable = new TestTable();
        testTable.setK(1);
        testTable.setM1(ImmutableMap.<String, Map<Integer, Integer>>of("bar", ImmutableMap.of(1, 2)));

        Set<Integer> s1 = Sets.newHashSet(1, 2, 3);
        Set<Integer> s2 = Sets.newHashSet(4, 5, 6);
        List<Set<Integer>> ls = new ArrayList<Set<Integer>>();
        ls.add(s1);
        ls.add(s2);

        testTable.setM2(ImmutableMap.<String, Map<Integer, TestType>>of("bar", ImmutableMap.of(1, new TestType(2, ls))));

        mapper.save(testTable);

        TestTable testTable2 = mapper.get(1);
        assertThat(testTable2.getM1()).isEqualTo(testTable.getM1());
        assertThat(testTable2.getM2()).isEqualTo(testTable.getM2());
    }

    @Test(groups = "short")
    public void should_map_accessor_params_to_nested_collections() {
        MappingManager manager = new MappingManager(session());
        Mapper<TestTable> mapper = manager.mapper(TestTable.class);
        TestAccessor accessor = manager.createAccessor(TestAccessor.class);

        TestTable testTable = new TestTable();
        testTable.setK(1);
        mapper.save(testTable);

        Map<String, Map<Integer, Integer>> m1 = ImmutableMap.<String, Map<Integer, Integer>>of("bar", ImmutableMap.of(1, 2));
        Map<String, Map<Integer, TestType>> m2 = ImmutableMap.<String, Map<Integer, TestType>>of("bar", ImmutableMap.of(1, new TestType(2)));
        accessor.setM1(1, m1);
        accessor.setM2(1, m2);

        TestTable testTable2 = mapper.get(1);
        assertThat(testTable2.getM1()).isEqualTo(m1);
        assertThat(testTable2.getM2()).isEqualTo(m2);
    }

    @UDT(name = "testType")
    public static class TestType {
        private int i;

        @Frozen("list<frozen<set<int>>>")
        private List<Set<Integer>> ls;

        public TestType() {
            ls = Lists.newArrayList();
        }

        public TestType(int i) {
            this();
            this.i = i;
        }

        public TestType(int i, List<Set<Integer>> ls) {
            this(i);
            this.ls = ls;
        }

        public int getI() {
            return i;
        }

        public void setI(int i) {
            this.i = i;
        }

        public List<Set<Integer>> getLs() {
            return ls;
        }

        public void setLs(List<Set<Integer>> ls) {
            this.ls = ls;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof TestType))
                return false;

            TestType testType = (TestType) o;

            return i == testType.i && ls.equals(testType.ls);

        }

        @Override
        public int hashCode() {
            int result = i;
            result = 31 * result + ls.hashCode();
            return result;
        }
    }

    @Table(name = "testTable")
    public static class TestTable {
        @PartitionKey
        private int k;

        @Frozen("map<text, frozen<map<int, int>>>")
        private Map<String, Map<Integer, Integer>> m1;

        @Frozen("map<text, frozen<map<int, frozen<testType>>>>")
        private Map<String, Map<Integer, TestType>> m2;

        public int getK() {
            return k;
        }

        public void setK(int k) {
            this.k = k;
        }

        public Map<String, Map<Integer, Integer>> getM1() {
            return m1;
        }

        public void setM1(Map<String, Map<Integer, Integer>> m1) {
            this.m1 = m1;
        }

        public Map<String, Map<Integer, TestType>> getM2() {
            return m2;
        }

        public void setM2(Map<String, Map<Integer, TestType>> m2) {
            this.m2 = m2;
        }
    }

    @Accessor
    public interface TestAccessor {
        @Query("UPDATE testTable SET m1 = :m1 WHERE k = :k")
        ResultSet setM1(@Param("k") int k, @Param("m1") Map<String, Map<Integer, Integer>> m1);

        @Query("UPDATE testTable SET m2 = :m2 WHERE k = :k")
        ResultSet setM2(@Param("k") int k, @Param("m2") Map<String, Map<Integer, TestType>> m2);
    }
}
