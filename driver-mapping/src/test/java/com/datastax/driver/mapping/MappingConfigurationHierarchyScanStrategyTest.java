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
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for JAVA-1310 - validate ability configure ancestor property scanning:
 * - disable
 * - configure max depth ancestor (included or not)
 */
public class MappingConfigurationHierarchyScanStrategyTest extends CCMTestsSupport {

    @Override
    public void onTestContextInitialized() {
        execute("CREATE TABLE foo (k int primary key, v int)");
        execute("INSERT INTO foo (k, v) VALUES (1, 1)");
    }

    @Test(groups = "short")
    public void should_not_inherit_annotations_when_hierarchy_scan_disabled() {
        MappingConfiguration conf = MappingConfiguration.builder()
                .withPropertyMapper(new DefaultPropertyMapper()
                        .setHierarchyScanStrategy(new MappedClassesOnlyHierarchyScanStrategy()))
                .build();
        MappingManager mappingManager = new MappingManager(session(), conf);
        mappingManager.mapper(Child1.class);
    }

    @SuppressWarnings({"unused", "WeakerAccess"})
    public static class Parent1 {

        @Column(name = "notAColumn")
        private int notAColumn;
    }

    @Table(name = "foo")
    @SuppressWarnings({"unused", "WeakerAccess"})
    public static class Child1 extends Parent1 {

        @PartitionKey
        private int k;

        public int getK() {
            return k;
        }

        public void setK(int k) {
            this.k = k;
        }
    }

    @Test(groups = "short")
    public void should_inherit_annotations_up_to_highest_ancestor_excluded() {
        MappingConfiguration conf = MappingConfiguration.builder()
                .withPropertyMapper(new DefaultPropertyMapper()
                        .setHierarchyScanStrategy(new DefaultHierarchyScanStrategy(GrandParent2.class, false)))
                .build();
        MappingManager mappingManager = new MappingManager(session(), conf);
        Mapper<Child2> mapper = mappingManager.mapper(Child2.class);
        assertThat(mapper.get(1).getV()).isEqualTo(1);
    }

    @Test(groups = "short")
    public void should_inherit_annotations_up_to_highest_ancestor_included() {
        MappingConfiguration conf = MappingConfiguration.builder()
                .withPropertyMapper(new DefaultPropertyMapper()
                        .setHierarchyScanStrategy(new DefaultHierarchyScanStrategy(Parent2.class, true)))
                .build();
        MappingManager mappingManager = new MappingManager(session(), conf);
        Mapper<Child2> mapper = mappingManager.mapper(Child2.class);
        assertThat(mapper.get(1).getV()).isEqualTo(1);
    }

    @SuppressWarnings({"unused", "WeakerAccess"})
    public static class GrandParent2 {

        @Column(name = "notAColumn")
        private int notAColumn;
    }

    @SuppressWarnings({"unused", "WeakerAccess"})
    public static class Parent2 extends GrandParent2 {

        private int v;

        public int getV() {
            return v;
        }

        public void setV(int v) {
            this.v = v;
        }
    }

    @Table(name = "foo")
    @SuppressWarnings({"unused", "WeakerAccess"})
    public static class Child2 extends Parent2 {

        @PartitionKey
        private int k;

        public int getK() {
            return k;
        }

        public void setK(int k) {
            this.k = k;
        }
    }
}
