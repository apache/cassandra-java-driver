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
package com.datastax.driver.core;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static com.datastax.driver.core.Assertions.assertThat;

public class FunctionMetadataTest {
    KeyspaceMetadata keyspace;
    private ProtocolVersion protocolVersion = TestUtils.getDesiredProtocolVersion();
    private CodecRegistry codecRegistry = new CodecRegistry();

    @BeforeMethod(groups = "unit")
    public void setup() {
        keyspace = new KeyspaceMetadata("ks", false, Collections.<String, String>emptyMap());
    }

    @Test(groups = "unit")
    public void should_parse_and_format_simple_function() {
        FunctionMetadata function = FunctionMetadata.build(keyspace, SYSTEM_ROW_PLUS, protocolVersion, codecRegistry);

        assertThat(function).isNotNull();
        assertThat(function.getKeyspace()).isEqualTo(keyspace);
        assertThat(function.getFullName()).isEqualTo("plus(int,int)");
        assertThat(function.getSimpleName()).isEqualTo("plus");
        assertThat(function.getReturnType()).isEqualTo(DataType.cint());
        assertThat(function.getArguments())
            .containsEntry("s", DataType.cint())
            .containsEntry("v", DataType.cint());
        assertThat(function.getLanguage()).isEqualTo("java");
        assertThat(function.getBody()).isEqualTo("return s+v;");
        assertThat(function.isCalledOnNullInput()).isFalse();

        assertThat(keyspace.getFunction("plus", DataType.cint(), DataType.cint())).isEqualTo(function);

        assertThat(function.toString())
            .isEqualTo("CREATE FUNCTION ks.plus(s int,v int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS 'return s+v;';");

        assertThat(function.exportAsString())
            .isEqualTo("CREATE FUNCTION ks.plus(\n"
                + "    s int,\n"
                + "    v int)\n"
                + "RETURNS NULL ON NULL INPUT\n"
                + "RETURNS int\n"
                + "LANGUAGE java\n"
                + "AS 'return s+v;';");
    }

    @Test(groups = "unit")
    public void should_parse_and_format_function_with_no_arguments() {
        FunctionMetadata function = FunctionMetadata.build(keyspace, SYSTEM_ROW_PI, protocolVersion, codecRegistry);

        assertThat(function).isNotNull();
        assertThat(function.getKeyspace()).isEqualTo(keyspace);
        assertThat(function.getFullName()).isEqualTo("pi()");
        assertThat(function.getSimpleName()).isEqualTo("pi");
        assertThat(function.getReturnType()).isEqualTo(DataType.cdouble());
        assertThat(function.getArguments()).isEmpty();
        assertThat(function.getLanguage()).isEqualTo("java");
        assertThat(function.getBody()).isEqualTo("return Math.PI;");
        assertThat(function.isCalledOnNullInput()).isTrue();

        assertThat(keyspace.getFunction("pi")).isEqualTo(function);

        assertThat(function.toString())
            .isEqualTo("CREATE FUNCTION ks.pi() CALLED ON NULL INPUT RETURNS double LANGUAGE java AS 'return Math.PI;';");

        assertThat(function.exportAsString())
            .isEqualTo("CREATE FUNCTION ks.pi()\n"
                + "CALLED ON NULL INPUT\n"
                + "RETURNS double\n"
                + "LANGUAGE java\n"
                + "AS 'return Math.PI;';");
    }

    private static final String INT = "org.apache.cassandra.db.marshal.Int32Type";
    private static final String DOUBLE = "org.apache.cassandra.db.marshal.DoubleType";

    private static final Row SYSTEM_ROW_PLUS = mockRow("plus",
        ImmutableList.of("int", "int"), ImmutableList.of("s", "v"), ImmutableList.of(INT, INT),
        "return s+v;", false, "java", INT
    );

    private static final Row SYSTEM_ROW_PI = mockRow("pi",
        Collections.<String>emptyList(), Collections.<String>emptyList(), Collections.<String>emptyList(),
        "return Math.PI;", true, "java", DOUBLE);

    private static Row mockRow(String functionName, List<String> signature, List<String> argumentNames, List<String> argumentTypes,
                               String body, boolean calledOnNullInput, String language, String returnType) {
        Row row = mock(Row.class);

        when(row.getString("function_name")).thenReturn(functionName);
        when(row.getList("signature", String.class)).thenReturn(signature);
        when(row.getList("argument_names", String.class)).thenReturn(argumentNames);
        when(row.getList("argument_types", String.class)).thenReturn(argumentTypes);
        when(row.getString("body")).thenReturn(body);
        when(row.getBool("called_on_null_input")).thenReturn(calledOnNullInput);
        when(row.getString("language")).thenReturn(language);
        when(row.getString("return_type")).thenReturn(returnType);

        return row;
    }
}
