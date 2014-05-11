package com.datastax.driver.core.schemabuilder;

import static org.assertj.core.api.Assertions.assertThat;
import org.testng.annotations.Test;

public class CreateIndexTest {

    @Test
    public void should_create_index() throws Exception {
        //Given //When
        final String statement = SchemaBuilder.createIndex("myIndex").ifNotExists().onTable("ks", "test").andColumn("col");

        //Then
        assertThat(statement).isEqualTo("\n\tCREATE INDEX IF NOT EXISTS myIndex ON ks.test(col)");
    }


}
