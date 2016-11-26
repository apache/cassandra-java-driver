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
import com.datastax.driver.mapping.annotations.Transient;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for JAVA-1310 - validate ability to transient properties at manager and mapper levels
 */
public class MapperConfigurationTransientTest extends CCMTestsSupport {

    @Override
    public void onTestContextInitialized() {
        execute("CREATE TABLE foo (k int primary key, v int)");
        execute("INSERT INTO foo (k, v) VALUES (1, 1)");
    }

    @Test(groups = "short")
    public void should_ignore_property_if_field_annotated_transient() {
        MappingManager mappingManager = new MappingManager(session());
        mappingManager.mapper(Foo1.class);
    }

    @Table(name = "foo")
    public static class Foo1 {
        @PartitionKey
        private int k;
        @Transient
        private int notAColumn;
    }

    @Test(groups = "short")
    public void should_ignore_property_if_getter_annotated_transient() {
        MappingManager mappingManager = new MappingManager(session());
        mappingManager.mapper(Foo2.class);
    }

    @Table(name = "foo")
    public static class Foo2 {
        @PartitionKey
        private int k;
        private int notAColumn;

        @Transient
        public int getNotAColumn() {
            return notAColumn;
        }
    }

    @Test(groups = "short")
    public void should_ignore_property_if_declared_transient_in_class_annotation() {
        MappingManager mappingManager = new MappingManager(session());
        mappingManager.mapper(Foo3.class);
    }

    @Table(name = "foo", transientProperties = {"class", "notAColumn"})
    public static class Foo3 {
        @PartitionKey
        private int k;
        private int notAColumn;
    }

    @Test(groups = "short")
    public void should_ignore_property_if_declared_transient_in_mapper_configuration() {
        MappingManager mappingManager = new MappingManager(session());
        MapperConfiguration configuration = new MapperConfiguration();
        MapperConfiguration.PropertyScanConfiguration scanConf = new MapperConfiguration.PropertyScanConfiguration();
        scanConf.setExcludedProperties(ImmutableSet.of("notAColumn"));
        configuration.setPropertyScanConfiguration(scanConf);
        mappingManager.mapper(Foo3.class, configuration);
    }

    @Table(name = "foo")
    public static class Foo4 {
        @PartitionKey
        private int k;
        private int notAColumn;
    }

}
