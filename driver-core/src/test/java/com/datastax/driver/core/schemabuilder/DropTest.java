package com.datastax.driver.core.schemabuilder;

import static org.assertj.core.api.Assertions.assertThat;
import org.testng.annotations.Test;

public class DropTest {

    @Test
    public void should_drop_table() throws Exception {
        //When
        final String built = SchemaBuilder.dropTable("test").build();

        //Then
        assertThat(built).isEqualTo("DROP TABLE test");
    }

    @Test
    public void should_drop_table_with_keyspace() throws Exception {
        //When
        final String built = SchemaBuilder.dropTable("ks", "test").build();

        //Then
        assertThat(built).isEqualTo("DROP TABLE ks.test");
    }

    @Test
    public void should_drop_table_with_keyspace_if_exists() throws Exception {
        //When
        final String built = SchemaBuilder.dropTable("ks", "test").ifExists(true).build();

        //Then
        assertThat(built).isEqualTo("DROP TABLE IF EXISTS ks.test");
    }
}
