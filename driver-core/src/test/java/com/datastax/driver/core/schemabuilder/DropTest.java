/*
 *      Copyright (C) 2012-2014 DataStax Inc.
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
package com.datastax.driver.core.schemabuilder;

import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

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

    @Test(expectedExceptions = IllegalArgumentException.class,
        expectedExceptionsMessageRegExp = "The keyspace name 'add' is not allowed because it is a reserved keyword")
    public void should_fail_if_keyspace_name_is_a_reserved_keyword() throws Exception {
        SchemaBuilder.dropTable("add","test").build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "The table name 'add' is not allowed because it is a reserved keyword")
    public void should_fail_if_table_name_is_a_reserved_keyword() throws Exception {
        SchemaBuilder.dropTable("add").build();
    }
}
