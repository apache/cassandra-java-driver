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

    @Test
    public void should_fail_if_keyspace_name_is_a_reserved_keyword() throws Exception {
        boolean hasFailed = false;
        try {
            SchemaBuilder.dropTable("add", "test").build();
        } catch (IllegalArgumentException e) {
            hasFailed = true;
            assertThat(e.getMessage()).isEqualTo("The keyspace name 'add' is not allowed because it is a reserved keyword");
        }

        assertThat(hasFailed).isTrue();
    }

    @Test
    public void should_fail_if_table_name_is_a_reserved_keyword() throws Exception {
        boolean hasFailed = false;
        try {
            SchemaBuilder.dropTable("add").build();
        } catch (IllegalArgumentException e) {
            hasFailed = true;
            assertThat(e.getMessage()).isEqualTo("The table name 'add' is not allowed because it is a reserved keyword");
        }

        assertThat(hasFailed).isTrue();
    }
}
