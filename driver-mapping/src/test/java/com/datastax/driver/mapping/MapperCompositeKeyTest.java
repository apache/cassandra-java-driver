package com.datastax.driver.mapping;

import java.util.Arrays;
import java.util.Collection;

import com.datastax.driver.mapping.annotations.*;
import org.testng.annotations.Test;

import com.datastax.driver.core.CCMBridge;

/**
 * Tests for the mapper with composite partition keys and multiple clustering columns.
 */
public class MapperCompositeKeyTest extends CCMBridge.PerClassSingleNodeCluster {

    protected Collection<String> getTableDefinitions() {
        return Arrays.asList("CREATE TABLE test_table (pk1 int, pk2 int, cc1 int, cc2 int, PRIMARY KEY ((pk1, pk2), cc1, cc2))");
    }

    @Table(keyspace = "ks", name = "test_table")
    public static class TestTable {
        @PartitionKey(0)
        private int pk1;

        @PartitionKey(1)
        private int pk2;

        @ClusteringColumn(0)
        private int cc1;

        @ClusteringColumn(1)
        private int cc2;

        public int getPk1() {
            return pk1;
        }

        public void setPk1(int pk1) {
            this.pk1 = pk1;
        }

        public int getPk2() {
            return pk2;
        }

        public void setPk2(int pk2) {
            this.pk2 = pk2;
        }

        public int getCc1() {
            return cc1;
        }

        public void setCc1(int cc1) {
            this.cc1 = cc1;
        }

        public int getCc2() {
            return cc2;
        }

        public void setCc2(int cc2) {
            this.cc2 = cc2;
        }
    }

    @Test(groups = "short")
    public void testCreateMapper() throws Exception {
        new MappingManager(session).mapper(TestTable.class);
    }
}
