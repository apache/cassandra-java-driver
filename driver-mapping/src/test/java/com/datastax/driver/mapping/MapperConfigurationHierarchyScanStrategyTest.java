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

import com.datastax.driver.core.CCMTestsSupport;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for JAVA-1310 - validate ability configure ancestor property scanning:
 * - disable
 * - force class annotations
 * - configure max depth ancestor
 */
public class MapperConfigurationHierarchyScanStrategyTest extends CCMTestsSupport {

    @Override
    public void onTestContextInitialized() {
        execute("CREATE TABLE foo (k int primary key, v int)");
        execute("INSERT INTO foo (k, v) VALUES (1, 1)");
    }

    @Test(groups = "short")
    public void should_not_inherit_properties() {
        MappingManager mappingManager = new MappingManager(session());
        MapperConfiguration conf = new MapperConfiguration();
        MapperConfiguration.PropertyScanConfiguration scanConf = new MapperConfiguration.PropertyScanConfiguration();
        MapperConfiguration.HierarchyScanStrategy scanStrategy = new MapperConfiguration.HierarchyScanStrategy();
        scanStrategy.setHierarchyScanEnabled(false);
        scanConf.setHierarchyScanStrategy(scanStrategy);
        conf.setPropertyScanConfiguration(scanConf);
        mappingManager.mapper(Foo1.class, conf);
    }

    @Table(name = "foo")
    public static class Boo1 {

        @Column(name = "notAColumn")
        private int notAColumn;

    }

    @Table(name = "foo")
    public static class Foo1 extends Boo1 {

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
    public void should_inherit_only_boo() {
        MappingManager mappingManager = new MappingManager(session());
        MapperConfiguration conf = new MapperConfiguration();
        MapperConfiguration.PropertyScanConfiguration scanConf = new MapperConfiguration.PropertyScanConfiguration();
        MapperConfiguration.HierarchyScanStrategy scanStrategy = new MapperConfiguration.HierarchyScanStrategy();
        scanStrategy.setDeepestAllowedAncestor(Boo2.class);
        scanConf.setHierarchyScanStrategy(scanStrategy);
        conf.setPropertyScanConfiguration(scanConf);
        Mapper<Foo2> mapper = mappingManager.mapper(Foo2.class, conf);
        assertThat(mapper.get(1).getV()).isEqualTo(1);
    }

    @Table(name = "foo")
    public static class Goo2 {

        @Column(name = "notAColumn")
        private int notAColumn;

    }

    @Table(name = "foo")
    public static class Boo2 extends Goo2 {

        private int v;

        public int getV() {
            return v;
        }

        public void setV(int v) {
            this.v = v;
        }

    }

    @Table(name = "foo")
    public static class Foo2 extends Boo2 {

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
    public void ignore_non_annotated_classes() {
        MappingManager mappingManager = new MappingManager(session());
        MapperConfiguration conf = new MapperConfiguration();
        MapperConfiguration.PropertyScanConfiguration scanConf = new MapperConfiguration.PropertyScanConfiguration();
        MapperConfiguration.HierarchyScanStrategy scanStrategy = new MapperConfiguration.HierarchyScanStrategy();
        scanStrategy.setScanOnlyAnnotatedClasses(true);
        scanConf.setHierarchyScanStrategy(scanStrategy);
        conf.setPropertyScanConfiguration(scanConf);
        Mapper<Foo3> mapper = mappingManager.mapper(Foo3.class, conf);
        assertThat(mapper.get(1).getV()).isEqualTo(1);
    }

    @Table(name = "foo")
    public static class Goo3 {

        private int v;

        public int getV() {
            return v;
        }

        public void setV(int v) {
            this.v = v;
        }

    }

    public static class Boo3 extends Goo3 {

        @Column(name = "notAColumn")
        private int notAColumn;

    }

    @Table(name = "foo")
    public static class Foo3 extends Boo3 {

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
