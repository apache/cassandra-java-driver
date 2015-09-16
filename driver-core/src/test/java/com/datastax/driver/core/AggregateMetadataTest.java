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
import static com.datastax.driver.core.DataType.text;

@CassandraVersion(major = 2.2)
public class AggregateMetadataTest extends CCMBridge.PerClassSingleNodeCluster {

    @Test(groups = "short")
    public void should_parse_and_format_aggregate_with_initcond_and_no_finalfunc() {
        // given
        String cqlFunction = String.format("CREATE FUNCTION %s.cat(s text,v int) RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE java AS 'return s+v;';", keyspace);
        String cqlAggregate = String.format("CREATE AGGREGATE %s.cat_tos(int) SFUNC cat STYPE text INITCOND '0';", keyspace);
        // when
        session.execute(cqlFunction);
        session.execute(cqlAggregate);
        // then
        KeyspaceMetadata keyspace = cluster.getMetadata().getKeyspace(this.keyspace);
        FunctionMetadata stateFunc = keyspace.getFunction("cat", text(), cint());
        AggregateMetadata aggregate = keyspace.getAggregate("cat_tos", cint());
        assertThat(aggregate).isNotNull();
        assertThat(aggregate.getFullName()).isEqualTo("cat_tos(int)");
        assertThat(aggregate.getSimpleName()).isEqualTo("cat_tos");
        assertThat(aggregate.getArgumentTypes()).containsExactly(cint());
        assertThat(aggregate.getFinalFunc()).isNull();
        assertThat(aggregate.getInitCond()).isEqualTo("0");
        assertThat(aggregate.getReturnType()).isEqualTo(text());
        assertThat(aggregate.getStateFunc()).isEqualTo(stateFunc);
        assertThat(aggregate.getStateType()).isEqualTo(text());
        assertThat(aggregate.toString()).isEqualTo(cqlAggregate);
        assertThat(aggregate.exportAsString()).isEqualTo(String.format("CREATE AGGREGATE %s.cat_tos(int)\n"
            + "SFUNC cat STYPE text\n"
            + "INITCOND '0';", this.keyspace));
    }

    @Test(groups = "short")
    public void should_parse_and_format_aggregate_with_no_arguments() {
        // given
        String cqlFunction = String.format("CREATE FUNCTION %s.inc(i int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS 'return i+1;';", keyspace);
        String cqlAggregate = String.format("CREATE AGGREGATE %s.mycount() SFUNC inc STYPE int INITCOND 0;", keyspace);
        // when
        session.execute(cqlFunction);
        session.execute(cqlAggregate);
        // then
        KeyspaceMetadata keyspace = cluster.getMetadata().getKeyspace(this.keyspace);
        FunctionMetadata stateFunc = keyspace.getFunction("inc", cint());
        AggregateMetadata aggregate = keyspace.getAggregate("mycount");
        assertThat(aggregate).isNotNull();
        assertThat(aggregate.getFullName()).isEqualTo("mycount()");
        assertThat(aggregate.getSimpleName()).isEqualTo("mycount");
        assertThat(aggregate.getArgumentTypes()).isEmpty();
        assertThat(aggregate.getFinalFunc()).isNull();
        assertThat(aggregate.getInitCond()).isEqualTo(0);
        assertThat(aggregate.getReturnType()).isEqualTo(cint());
        assertThat(aggregate.getStateFunc()).isEqualTo(stateFunc);
        assertThat(aggregate.getStateType()).isEqualTo(cint());
        assertThat(aggregate.toString()).isEqualTo(cqlAggregate);
        assertThat(aggregate.exportAsString()).isEqualTo(String.format("CREATE AGGREGATE %s.mycount()\n"
            + "SFUNC inc STYPE int\n"
            + "INITCOND 0;", this.keyspace));
    }

    @Test(groups = "short")
    public void should_parse_and_format_aggregate_with_final_function() {
        // given
        String cqlFunction1 = String.format("CREATE FUNCTION %s.plus(i int, j int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS 'return i+j;';", keyspace);
        String cqlFunction2 = String.format("CREATE FUNCTION %s.announce(i int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS 'return i;';", keyspace);
        String cqlAggregate = String.format("CREATE AGGREGATE %s.prettysum(int) SFUNC plus STYPE int FINALFUNC announce INITCOND 0;", keyspace);
        // when
        session.execute(cqlFunction1);
        session.execute(cqlFunction2);
        session.execute(cqlAggregate);
        // then
        KeyspaceMetadata keyspace = cluster.getMetadata().getKeyspace(this.keyspace);
        FunctionMetadata stateFunc = keyspace.getFunction("plus", cint(), cint());
        FunctionMetadata finalFunc = keyspace.getFunction("announce", cint());
        AggregateMetadata aggregate = keyspace.getAggregate("prettysum", cint());
        assertThat(aggregate).isNotNull();
        assertThat(aggregate.getFullName()).isEqualTo("prettysum(int)");
        assertThat(aggregate.getSimpleName()).isEqualTo("prettysum");
        assertThat(aggregate.getArgumentTypes()).containsExactly(cint());
        assertThat(aggregate.getFinalFunc()).isEqualTo(finalFunc);
        assertThat(aggregate.getInitCond()).isEqualTo(0);
        assertThat(aggregate.getReturnType()).isEqualTo(cint());
        assertThat(aggregate.getStateFunc()).isEqualTo(stateFunc);
        assertThat(aggregate.getStateType()).isEqualTo(cint());
        assertThat(aggregate.toString()).isEqualTo(cqlAggregate);
        assertThat(aggregate.exportAsString()).isEqualTo(String.format("CREATE AGGREGATE %s.prettysum(int)\n"
            + "SFUNC plus STYPE int\n"
            + "FINALFUNC announce\n"
            + "INITCOND 0;", this.keyspace));
    }

    @Test(groups = "short")
    public void should_parse_and_format_aggregate_with_no_initcond() {
        // given
        String cqlFunction = String.format("CREATE FUNCTION %s.plus2(i int, j int) CALLED ON NULL INPUT RETURNS int LANGUAGE java AS 'return i+j;';", keyspace);
        String cqlAggregate = String.format("CREATE AGGREGATE %s.sum(int) SFUNC plus2 STYPE int;", keyspace);
        // when
        session.execute(cqlFunction);
        session.execute(cqlAggregate);
        // then
        KeyspaceMetadata keyspace = cluster.getMetadata().getKeyspace(this.keyspace);
        FunctionMetadata stateFunc = keyspace.getFunction("plus2", cint(), cint());
        AggregateMetadata aggregate = keyspace.getAggregate("sum", cint());
        assertThat(aggregate).isNotNull();
        assertThat(aggregate.getFullName()).isEqualTo("sum(int)");
        assertThat(aggregate.getSimpleName()).isEqualTo("sum");
        assertThat(aggregate.getArgumentTypes()).containsExactly(cint());
        assertThat(aggregate.getFinalFunc()).isNull();
        assertThat(aggregate.getInitCond()).isNull();
        assertThat(aggregate.getReturnType()).isEqualTo(cint());
        assertThat(aggregate.getStateFunc()).isEqualTo(stateFunc);
        assertThat(aggregate.getStateType()).isEqualTo(cint());
        assertThat(aggregate.toString()).isEqualTo(cqlAggregate);
        assertThat(aggregate.exportAsString()).isEqualTo(String.format("CREATE AGGREGATE %s.sum(int)\n"
            + "SFUNC plus2 STYPE int;", this.keyspace));
    }

    @Override
    protected Collection<String> getTableDefinitions() {
        return Collections.emptyList();
    }

}
