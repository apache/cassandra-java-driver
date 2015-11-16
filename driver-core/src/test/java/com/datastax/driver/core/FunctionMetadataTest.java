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

import java.util.Collection;
import java.util.Collections;

import org.testng.annotations.Test;

import com.datastax.driver.core.utils.CassandraVersion;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.DataType.cint;

@CassandraVersion(major = 2.2)
public class FunctionMetadataTest extends CCMBridge.PerClassSingleNodeCluster {

    @Test(groups = "short")
    public void should_parse_and_format_simple_function() {
        // given
        String cql = String.format("CREATE FUNCTION %s.plus(s int,v int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS 'return s+v;';", keyspace);
        // when
        session.execute(cql);
        // then
        KeyspaceMetadata keyspace = cluster.getMetadata().getKeyspace(this.keyspace);
        FunctionMetadata function = keyspace.getFunction("plus", cint(), cint());
        assertThat(function).isNotNull();
        assertThat(function.getKeyspace()).isEqualTo(keyspace);
        assertThat(function.getFullName()).isEqualTo("plus(int,int)");
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
        session.execute(cql);
        // then
        KeyspaceMetadata keyspace = cluster.getMetadata().getKeyspace(this.keyspace);
        FunctionMetadata function = keyspace.getFunction("pi");
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
            .isEqualTo(cql);
        assertThat(function.exportAsString())
            .isEqualTo(String.format("CREATE FUNCTION %s.pi()\n"
                + "CALLED ON NULL INPUT\n"
                + "RETURNS double\n"
                + "LANGUAGE java\n"
                + "AS 'return Math.PI;';", this.keyspace));
    }

    @Override
    protected Collection<String> getTableDefinitions() {
        return Collections.emptyList();
    }

}
