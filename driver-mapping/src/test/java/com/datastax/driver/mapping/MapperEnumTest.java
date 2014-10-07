package com.datastax.driver.mapping;

import java.util.Collection;

import com.datastax.driver.mapping.annotations.Enumerated;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import static org.junit.Assert.assertNull;

import com.datastax.driver.core.CCMBridge;

/**
 * Covers handling of enum fields by the mapper.
 */
public class MapperEnumTest extends CCMBridge.PerClassSingleNodeCluster {

    @Override
    protected Collection<String> getTableDefinitions() {
        return Lists.newArrayList("CREATE TABLE asOrdinal (k int primary key, v int)",
                                  "CREATE TABLE asString (k int primary key, v text)");
    }

    public enum Enum {
        FOO, BAR
    }

    @Test(groups = "short")
    public void should_handle_null_enum_as_ordinal() {
        Mapper<AsOrdinal> mapper = new MappingManager(session).mapper(AsOrdinal.class);

        mapper.save(new AsOrdinal(1, null));
        AsOrdinal o = mapper.get(1);

        assertNull(o.getV());
    }

    @Test(groups = "short")
    public void should_handle_null_enum_as_string() {
        Mapper<AsString> mapper = new MappingManager(session).mapper(AsString.class);

        mapper.save(new AsString(1, null));
        AsString o = mapper.get(1);

        assertNull(o.getV());
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
}
