package com.datastax.driver.mapping;

import java.util.Collection;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import com.datastax.driver.core.CCMBridge;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.mapping.annotations.*;

import static com.datastax.driver.core.Assertions.assertThat;

public class MapperNestedCollectionsTest extends CCMBridge.PerClassSingleNodeCluster {

    @Override
    protected Collection<String> getTableDefinitions() {
        return Lists.newArrayList(
            "CREATE TYPE testType(i int)",
            "CREATE TABLE testTable (k int primary key, "
                + "m1 map<text, frozen<map<int, int>>>, " // nested collection
                + "m2 map<text, frozen<map<int, frozen<testType>>>>)" // nested collection with UDT
        );
    }

    @Test(groups = "short")
    public void should_map_fields_to_nested_collections() {
        Mapper<TestTable> mapper = new MappingManager(session).mapper(TestTable.class);

        TestTable testTable = new TestTable();
        testTable.setK(1);
        testTable.setM1(ImmutableMap.<String, Map<Integer, Integer>>of("bar", ImmutableMap.of(1, 2)));
        testTable.setM2(ImmutableMap.<String, Map<Integer, TestType>>of("bar", ImmutableMap.of(1, new TestType(2))));

        mapper.save(testTable);

        TestTable testTable2 = mapper.get(1);
        assertThat(testTable2.getM1()).isEqualTo(testTable.getM1());
        assertThat(testTable2.getM2()).isEqualTo(testTable.getM2());
    }

    @Test(groups = "short")
    public void should_map_accessor_params_to_nested_collections() {
        MappingManager manager = new MappingManager(session);
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

    @UDT(keyspace = "ks", name = "testType")
    public static class TestType {
        private int i;

        public TestType() {
        }

        public TestType(int i) {
            this.i = i;
        }

        public int getI() {
            return i;
        }

        public void setI(int i) {
            this.i = i;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other)
                return true;
            if (other instanceof TestType) {
                TestType that = (TestType)other;
                return this.i == that.i;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return i;
        }
    }

    @Table(keyspace = "ks", name = "testTable")
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
    public static interface TestAccessor {
        @Query("UPDATE testTable SET m1 = :m1 WHERE k = :k")
        ResultSet setM1(@Param("k") int k, @Param("m1") Map<String, Map<Integer, Integer>> m1);

        @Query("UPDATE testTable SET m2 = :m2 WHERE k = :k")
        ResultSet setM2(@Param("k") int k, @Param("m2") Map<String, Map<Integer, TestType>> m2);
    }
}
