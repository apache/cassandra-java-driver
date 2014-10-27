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

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.UserType;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CreateTypeTest {

    @Test
    public void should_create_UDT() throws Exception {
        //When
        final String built = SchemaBuilder.createType("ks", "myType")
                .addColumn("col1", DataType.text())
                .addColumn("col2", DataType.bigint())
                .build();

        //Then
        assertThat(built).isEqualTo("\n\tCREATE TYPE ks.myType(\n\t\t" +
                "col1 text,\n\t\t" +
                "col2 bigint)");
    }

    @Test
    public void should_create_UDT_if_not_exists() throws Exception {
        //When
        final String built = SchemaBuilder.createType("myType")
                .ifNotExists(true)
                .addColumn("col1", DataType.text())
                .addColumn("col2", DataType.bigint())
                .build();

        //Then
        assertThat(built).isEqualTo("\n\tCREATE TYPE IF NOT EXISTS myType(\n\t\t" +
                "col1 text,\n\t\t" +
                "col2 bigint)");
    }

    @Test
    public void should_create_simple_UDT_column() throws Exception {
        //When
        final String built = SchemaBuilder.createType("ks", "myType")
                .addColumn("col1", DataType.text())
                .addUDTColumn("my_udt", "address")
                .build();


        //Then
        assertThat(built).isEqualTo("\n\tCREATE TYPE ks.myType(\n\t\t" +
                "col1 text,\n\t\t" +
                "my_udt frozen<address>)");
    }

    @Test
    public void should_create_list_UDT_column() throws Exception {
        //When
        final String built = SchemaBuilder.createType("ks", "myType")
                .addColumn("col1", DataType.text())
                .addUDTListColumn("my_udt", "address")
                .build();


        //Then
        assertThat(built).isEqualTo("\n\tCREATE TYPE ks.myType(\n\t\t" +
                "col1 text,\n\t\t" +
                "my_udt list<frozen<address>>)");
    }

    @Test
    public void should_create_set_UDT_column() throws Exception {
        //When
        final String built = SchemaBuilder.createType("ks", "myType")
                .addColumn("col1", DataType.text())
                .addUDTSetColumn("my_udt", "address")
                .build();


        //Then
        assertThat(built).isEqualTo("\n\tCREATE TYPE ks.myType(\n\t\t" +
                "col1 text,\n\t\t" +
                "my_udt set<frozen<address>>)");
    }

    @Test
    public void should_create_key_UDT_map_column() throws Exception {
        //When
        final String built = SchemaBuilder.createType("ks", "myType")
                .addColumn("col1", DataType.text())
                .addUDTMapColumn("my_udt", "address", DataType.text())
                .build();


        //Then
        assertThat(built).isEqualTo("\n\tCREATE TYPE ks.myType(\n\t\t" +
                "col1 text,\n\t\t" +
                "my_udt map<frozen<address>, text>)");
    }

    @Test
    public void should_create_value_UDT_map_column() throws Exception {
        //When
        final String built = SchemaBuilder.createType("ks", "myType")
                .addColumn("col1", DataType.text())
                .addUDTMapColumn("my_udt", DataType.cint(),"address")
                .build();


        //Then
        assertThat(built).isEqualTo("\n\tCREATE TYPE ks.myType(\n\t\t" +
                "col1 text,\n\t\t" +
                "my_udt map<int, frozen<address>>)");
    }

    @Test
    public void should_create_key_value_UDT_map_column() throws Exception {
        //When
        final String built = SchemaBuilder.createType("ks", "myType")
                .addColumn("col1", DataType.text())
                .addUDTMapColumn("my_udt", "coords","address")
                .build();


        //Then
        assertThat(built).isEqualTo("\n\tCREATE TYPE ks.myType(\n\t\t" +
                "col1 text,\n\t\t" +
                "my_udt map<frozen<coords>, frozen<address>>)");
    }

    @Test
    public void should_create_column_with_manual_type() throws Exception {
        //When
        final String built = SchemaBuilder.createType("ks", "myType")
                .addColumn("col1", DataType.text())
                .addColumn("my_udt", "frozen<address>")
                .build();


        //Then
        assertThat(built).isEqualTo("\n\tCREATE TYPE ks.myType(\n\t\t" +
                "col1 text,\n\t\t" +
                "my_udt frozen<address>)");
    }
}