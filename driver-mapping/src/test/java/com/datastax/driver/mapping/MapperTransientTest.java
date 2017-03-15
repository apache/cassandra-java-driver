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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MapperTransientTest extends CCMTestsSupport {
    private MappingManager mappingManager;

    @Override
    public void onTestContextInitialized() {
        execute("CREATE TABLE foo (k int primary key, v int)");
        execute("INSERT INTO foo (k, v) VALUES (1, 1)");
    }

    @BeforeClass
    public void setup() {
        mappingManager = new MappingManager(session());
    }

    @Test(groups = "short")
    public void should_ignore_property_if_field_annotated_transient() {
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
        mappingManager.mapper(Foo3.class);
    }

    @Table(name = "foo", transientProperties = {"class", "notAColumn"})
    public static class Foo3 {
        @PartitionKey
        private int k;
        private int notAColumn;
    }

    @Test(groups = "short")
    public void should_not_ignore_property_if_ignored_at_class_level_but_annotated() {
        Foo4 foo = mappingManager.mapper(Foo4.class).get(1);
        assertThat(foo.getV()).isEqualTo(1);
    }

    @Table(name = "foo", transientProperties = {"class", "v"})
    public static class Foo4 {
        @PartitionKey
        private int k;
        @Column
        private int v;

        public int getK() {
            return k;
        }

        public void setK(int k) {
            this.k = k;
        }

        public int getV() {
            return v;
        }

        public void setV(int v) {
            this.v = v;
        }
    }
}
