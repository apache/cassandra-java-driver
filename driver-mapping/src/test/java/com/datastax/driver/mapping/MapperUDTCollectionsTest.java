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
import com.datastax.driver.core.utils.CassandraVersion;
import com.datastax.driver.core.utils.MoreObjects;
import com.datastax.driver.mapping.annotations.*;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.testng.Assert.assertEquals;

/**
 * Tests mapping of collections of UDTs.
 */
@SuppressWarnings("unused")
@CassandraVersion("2.1.0")
public class MapperUDTCollectionsTest extends CCMTestsSupport {

    @Override
    public void onTestContextInitialized() {
        execute("CREATE TYPE \"Sub\"(i int)",
                "CREATE TABLE collection_examples (id int PRIMARY KEY, " +
                        "l list<frozen<\"Sub\">>, " +
                        "s set<frozen<\"Sub\">>, " +
                        "m1 map<int,frozen<\"Sub\">>, " +
                        "m2 map<frozen<\"Sub\">,int>, " +
                        "m3 map<frozen<\"Sub\">,frozen<\"Sub\">>)");
    }

    @UDT(name = "Sub", caseSensitiveType = true)
    public static class Sub {
        private int i;

        public Sub() {
        }

        public Sub(int i) {
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
            if (other instanceof Sub) {
                Sub that = (Sub) other;
                return this.i == that.i;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return MoreObjects.hashCode(i);
        }
    }

    @Table(name = "collection_examples")
    public static class CollectionExamples {
        @PartitionKey
        private int id;

        @FrozenValue
        private List<Sub> l;

        @FrozenValue
        private Set<Sub> s;

        @FrozenValue
        private Map<Integer, Sub> m1;

        @FrozenKey
        private Map<Sub, Integer> m2;

        @FrozenKey
        @FrozenValue
        private Map<Sub, Sub> m3;

        public CollectionExamples() {
        }

        public CollectionExamples(int id, int value) {
            this.id = id;
            // Just fill the collections with random values
            Sub sub1 = new Sub(value);
            Sub sub2 = new Sub(value + 1);
            this.l = Lists.newArrayList(sub1, sub2);
            this.s = Sets.newHashSet(sub1, sub2);
            this.m1 = ImmutableMap.of(1, sub1, 2, sub2);
            this.m2 = ImmutableMap.of(sub1, 1, sub2, 2);
            this.m3 = ImmutableMap.of(sub1, sub1, sub2, sub2);
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public List<Sub> getL() {
            return l;
        }

        public void setL(List<Sub> l) {
            this.l = l;
        }

        public Set<Sub> getS() {
            return s;
        }

        public void setS(Set<Sub> s) {
            this.s = s;
        }

        public Map<Integer, Sub> getM1() {
            return m1;
        }

        public void setM1(Map<Integer, Sub> m1) {
            this.m1 = m1;
        }

        public Map<Sub, Integer> getM2() {
            return m2;
        }

        public void setM2(Map<Sub, Integer> m2) {
            this.m2 = m2;
        }

        public Map<Sub, Sub> getM3() {
            return m3;
        }

        public void setM3(Map<Sub, Sub> m3) {
            this.m3 = m3;
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof CollectionExamples) {
                CollectionExamples that = (CollectionExamples) other;
                return MoreObjects.equal(this.id, that.id) &&
                        MoreObjects.equal(this.l, that.l) &&
                        MoreObjects.equal(this.s, that.s) &&
                        MoreObjects.equal(this.m1, that.m1) &&
                        MoreObjects.equal(this.m2, that.m2) &&
                        MoreObjects.equal(this.m3, that.m3);
            }
            return false;
        }
    }

    @Test(groups = "short")
    public void testCollections() throws Exception {
        Mapper<CollectionExamples> m = new MappingManager(session()).mapper(CollectionExamples.class);

        CollectionExamples c = new CollectionExamples(1, 1);
        m.save(c);

        assertEquals(m.get(c.getId()), c);
    }

    @Test(groups = "short")
    public void testNullCollection() {
        Mapper<CollectionExamples> m = new MappingManager(session()).mapper(CollectionExamples.class);

        CollectionExamples c = new CollectionExamples(1, 1);
        c.setL(null);
        c.setS(null);
        c.setM1(null);
        c.setM2(null);
        c.setM3(null);
        m.save(c);

        assertEquals(m.get(c.getId()), c);
    }
}
