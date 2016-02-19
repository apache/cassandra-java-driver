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
import com.datastax.driver.mapping.annotations.Enumerated;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNull;

/**
 * Covers handling of enum fields by the mapper.
 */
@SuppressWarnings("unused")
public class MapperEnumTest extends CCMTestsSupport {

    @Override
    public void onTestContextInitialized() {
        execute("CREATE TABLE asOrdinal (k int primary key, v int)",
                "CREATE TABLE asString (k int primary key, v text)",
                "CREATE TABLE asPk (k int primary key, v int)");
    }

    public enum Enum {
        FOO, BAR
    }

    @Test(groups = "short")
    public void should_handle_null_enum_as_ordinal() {
        Mapper<AsOrdinal> mapper = new MappingManager(session()).mapper(AsOrdinal.class);

        mapper.save(new AsOrdinal(1, null));
        AsOrdinal o = mapper.get(1);

        assertNull(o.getV());
    }

    @Test(groups = "short")
    public void should_handle_null_enum_as_string() {
        Mapper<AsString> mapper = new MappingManager(session()).mapper(AsString.class);

        mapper.save(new AsString(1, null));
        AsString o = mapper.get(1);

        assertNull(o.getV());
    }

    /**
     * Ensures that an entity that has an Enum as part of its primary key
     * can be successfully stored and retrieved.
     *
     * @jira_ticket JAVA-831
     */
    @Test(groups = "short")
    public void should_handle_enum_as_primary_key() {
        Mapper<AsPk> mapper = new MappingManager(session()).mapper(AsPk.class);

        mapper.save(new AsPk(Enum.FOO, 42));
        AsPk o = mapper.get(Enum.FOO);

        assertThat(o.getV()).isEqualTo(42);
        mapper.delete(Enum.FOO);

        assertThat(mapper.get(Enum.FOO)).isNull();
    }

    @Table(name = "asOrdinal")
    public static class AsOrdinal {
        @PartitionKey
        private int k;

        @Enumerated(EnumType.ORDINAL)
        private Enum v;

        public AsOrdinal(Integer k, Enum v) {
            this.k = k;
            this.v = v;
        }

        public AsOrdinal() {
        }

        public int getK() {
            return k;
        }

        public void setK(int k) {
            this.k = k;
        }

        public Enum getV() {
            return v;
        }

        public void setV(Enum v) {
            this.v = v;
        }
    }

    @Table(name = "asString")
    public static class AsString {
        @PartitionKey
        private int k;

        @Enumerated(EnumType.STRING)
        private Enum v;

        public AsString(Integer k, Enum v) {
            this.k = k;
            this.v = v;
        }

        public AsString() {
        }

        public int getK() {
            return k;
        }

        public void setK(int k) {
            this.k = k;
        }

        public Enum getV() {
            return v;
        }

        public void setV(Enum v) {
            this.v = v;
        }
    }

    @Table(name = "asPk")
    public static class AsPk {

        @PartitionKey
        @Enumerated(EnumType.ORDINAL)
        private Enum k;

        private int v;

        public AsPk(Enum k, int v) {
            this.k = k;
            this.v = v;
        }

        public AsPk() {
        }

        public Enum getK() {
            return k;
        }

        public void setK(Enum k) {
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
