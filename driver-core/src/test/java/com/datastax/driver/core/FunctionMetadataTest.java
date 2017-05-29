/*
 * Copyright (C) 2012-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core;

import com.datastax.driver.core.utils.CassandraVersion;
import org.testng.annotations.Test;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.DataType.cint;
import static com.datastax.driver.core.DataType.map;
import static org.assertj.core.api.Assertions.entry;

@CassandraVersion("2.2.0")
@CCMConfig(config = "enable_user_defined_functions:true")
public class FunctionMetadataTest extends CCMTestsSupport {

    @Test(groups = "short")
    public void should_parse_and_format_simple_function() {
        // given
        String cql = String.format("CREATE FUNCTION %s.plus(s int,v int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS 'return s+v;';", keyspace);
        // when
        session().execute(cql);
        // then
        KeyspaceMetadata keyspace = cluster().getMetadata().getKeyspace(this.keyspace);
        FunctionMetadata function = keyspace.getFunction("plus", cint(), cint());
        assertThat(function).isNotNull();
        assertThat(function.getKeyspace()).isEqualTo(keyspace);
        assertThat(function.getSignature()).isEqualTo("plus(int,int)");
        assertThat(function.getSimpleName()).isEqualTo("plus");
        assertThat(function.getReturnType()).isEqualTo(cint());
        assertThat(function.getArguments())
                .containsEntry("s", cint())
                .containsEntry("v", cint());
        assertThat(function.getLanguage()).isEqualTo("java");
        assertThat(function.getBody()).isEqualTo("return s+v;");
        assertThat(function.isCalledOnNullInput()).isFalse();
        assertThat(function.toString())
                .isEqualTo(cql);
        assertThat(function.exportAsString())
                .isEqualTo(String.format("CREATE FUNCTION %s.plus(\n"
                        + "    s int,\n"
                        + "    v int)\n"
                        + "RETURNS NULL ON NULL INPUT\n"
                        + "RETURNS int\n"
                        + "LANGUAGE java\n"
                        + "AS 'return s+v;';", this.keyspace));
    }

    @Test(groups = "short")
    public void should_parse_and_format_function_with_no_arguments() {
        // given
        String cql = String.format("CREATE FUNCTION %s.pi() CALLED ON NULL INPUT RETURNS double LANGUAGE java AS 'return Math.PI;';", keyspace);
        // when
        session().execute(cql);
        // then
        KeyspaceMetadata keyspace = cluster().getMetadata().getKeyspace(this.keyspace);
        FunctionMetadata function = keyspace.getFunction("pi");
        assertThat(function).isNotNull();
        assertThat(function.getKeyspace()).isEqualTo(keyspace);
        assertThat(function.getSignature()).isEqualTo("pi()");
        assertThat(function.getSimpleName()).isEqualTo("pi");
        assertThat(function.getReturnType()).isEqualTo(DataType.cdouble());
        assertThat(function.getArguments()).isEmpty();
        assertThat(function.getLanguage()).isEqualTo("java");
        assertThat(function.getBody()).isEqualTo("return Math.PI;");
        assertThat(function.isCalledOnNullInput()).isTrue();
        assertThat(function.toString())
                .isEqualTo(cql);
        assertThat(function.exportAsString())
                .isEqualTo(String.format("CREATE FUNCTION %s.pi()\n"
                        + "CALLED ON NULL INPUT\n"
                        + "RETURNS double\n"
                        + "LANGUAGE java\n"
                        + "AS 'return Math.PI;';", this.keyspace));
    }

    @Test(groups = "short")
    public void should_parse_and_format_function_with_udts() {
        // given
        String body =
                "//If \"called on null input\", handle nulls\n"
                        + "if(ADDRESS == null) return previous_total + 0;\n"
                        + "//User types are converted to com.datastax.driver.core.UDTValue types\n"
                        + "java.util.Set phones = ADDRESS.getSet(\"phones\", com.datastax.driver.core.UDTValue.class);\n"
                        + "return previous_total + phones.size();\n";
        String cqlFunction = String.format(
                "CREATE FUNCTION %s.\"NUM_PHONES_ACCU\"(previous_total int,\"ADDRESS\" \"Address\") "
                        + "CALLED ON NULL INPUT "
                        + "RETURNS int "
                        + "LANGUAGE java "
                        + "AS "
                        + "'"
                        + body
                        + "';", keyspace);
        // when
        session().execute(cqlFunction);
        // then
        KeyspaceMetadata keyspace = cluster().getMetadata().getKeyspace(this.keyspace);
        UserType addressType = keyspace.getUserType("\"Address\"");
        FunctionMetadata function = keyspace.getFunction("\"NUM_PHONES_ACCU\"", cint(), addressType);
        assertThat(function).isNotNull();
        assertThat(function.getKeyspace()).isEqualTo(keyspace);

        assertThat(function.getSignature()).isEqualTo("\"NUM_PHONES_ACCU\"(int,\"Address\")");
        assertThat(function.getSimpleName()).isEqualTo("NUM_PHONES_ACCU");
        assertThat(function.getReturnType()).isEqualTo(cint());
        assertThat(function.getArguments()).containsExactly(entry("previous_total", cint()), entry("ADDRESS", addressType));
        assertThat(function.getLanguage()).isEqualTo("java");
        assertThat(function.getBody()).isEqualTo(body);
        assertThat(function.isCalledOnNullInput()).isTrue();
        assertThat(function.toString()).isEqualTo(cqlFunction);
    }

    /**
     * Ensures that functions whose arguments contain complex types such as
     * tuples and collections, and nested combinations thereof, are
     * correctly parsed.
     *
     * @jira_ticket JAVA-1137
     */
    @Test(groups = "short")
    public void should_parse_and_format_functions_with_complex_arguments() {
        // given
        String cql = String.format("CREATE FUNCTION %s.complex(x tuple<tuple<int>, map<int, int>>) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS 'return 42;';", keyspace);
        // when
        session().execute(cql);
        // then
        KeyspaceMetadata keyspace = cluster().getMetadata().getKeyspace(this.keyspace);
        DataType argumentType = cluster().getMetadata().newTupleType(cluster().getMetadata().newTupleType(cint()), map(cint(), cint()));
        FunctionMetadata function = keyspace.getFunction("complex", argumentType);
        assertThat(function).isNotNull();
        assertThat(function.getKeyspace()).isEqualTo(keyspace);
        assertThat(function.getSignature()).isEqualTo("complex(tuple<tuple<int>, map<int, int>>)");
        assertThat(function.getSimpleName()).isEqualTo("complex");
        assertThat(function.getReturnType()).isEqualTo(cint());
        assertThat(function.getArguments())
                .containsEntry("x", argumentType);
        assertThat(function.getLanguage()).isEqualTo("java");
        assertThat(function.getBody()).isEqualTo("return 42;");
        assertThat(function.isCalledOnNullInput()).isFalse();
        assertThat(function.toString())
                .isEqualTo(cql);
        assertThat(function.exportAsString())
                .isEqualTo(String.format("CREATE FUNCTION %s.complex(\n"
                        + "    x tuple<tuple<int>, map<int, int>>)\n"
                        + "RETURNS NULL ON NULL INPUT\n"
                        + "RETURNS int\n"
                        + "LANGUAGE java\n"
                        + "AS 'return 42;';", this.keyspace));
    }

    @Override
    public void onTestContextInitialized() {
        execute(
                String.format("CREATE TYPE IF NOT EXISTS %s.\"Phone\" (number text)", keyspace),
                String.format("CREATE TYPE IF NOT EXISTS %s.\"Address\" ("
                        + "    street text,"
                        + "    city text,"
                        + "    zip int,"
                        + "    phones frozen<set<frozen<\"Phone\">>>,"
                        + "    location frozen<tuple<float, float>>"
                        + ")", keyspace)
        );
    }

}
