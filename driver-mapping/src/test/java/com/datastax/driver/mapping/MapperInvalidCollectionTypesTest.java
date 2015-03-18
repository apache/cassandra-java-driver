package com.datastax.driver.mapping;

import java.util.*;

import org.testng.annotations.Test;

import com.datastax.driver.core.CCMBridge;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

public class MapperInvalidCollectionTypesTest extends CCMBridge.PerClassSingleNodeCluster {
    protected Collection<String> getTableDefinitions() {
        return Arrays.asList(
            "CREATE TABLE table_list (id int PRIMARY KEY, l list<int>)",
            "CREATE TABLE table_set (id int PRIMARY KEY, s set<int>)",
            "CREATE TABLE table_map (id int PRIMARY KEY, m map<int, int>)");
    }

    @Table(keyspace="ks", name="table_list")
    public class InvalidList {
        @PartitionKey
        private int id;

        private ArrayList<Integer> l;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public ArrayList<Integer> getL() {
            return l;
        }

        public void setL(ArrayList<Integer> l) {
            this.l = l;
        }
    }

    @Table(keyspace="ks", name="table_set")
    public class InvalidSet {
        @PartitionKey
        private int id;

        private HashSet<Integer> s;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public HashSet<Integer> getS() {
            return s;
        }

        public void setS(HashSet<Integer> s) {
            this.s = s;
        }
    }

    @Table(keyspace="ks", name="table_map")
    public class InvalidMap {
        @PartitionKey
        private int id;

        private TreeMap<Integer, Integer> m;

        public TreeMap<Integer, Integer> getM() {
            return m;
        }

        public void setM(TreeMap<Integer, Integer> m) {
            this.m = m;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }
    }

    @Test(groups="short", expectedExceptions = IllegalArgumentException.class)
    public void list_type_is_enforced() {
        new MappingManager(session).mapper(InvalidList.class);
    }

    @Test(groups="short", expectedExceptions = IllegalArgumentException.class)
    public void map_type_is_enforced() {
        new MappingManager(session).mapper(InvalidMap.class);
    }

    @Test(groups="short", expectedExceptions = IllegalArgumentException.class)
    public void set_type_is_enforced() {
        new MappingManager(session).mapper(InvalidSet.class);
    }
}
