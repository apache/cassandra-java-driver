package com.datastax.driver.core;

import java.util.Collection;

import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TableMetadataTest extends CCMBridge.PerClassSingleNodeCluster {
    @Override
    protected Collection<String> getTableDefinitions() {
        return Lists.newArrayList(
            "CREATE TABLE single_quote (\n"
                + "    c1 int PRIMARY KEY\n"
                + ") WITH  comment = 'comment with single quote '' should work'"
        );
    }

    @Test(groups = "short")
    public void should_escape_single_quote_table_comment() {
        TableMetadata table = cluster.getMetadata().getKeyspace("ks").getTable("single_quote");
        assertThat(table.asCQLQuery()).contains("comment with single quote '' should work");
    }
}