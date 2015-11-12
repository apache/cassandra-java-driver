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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static com.datastax.driver.core.Assertions.assertThat;

public class AggregateMetadataTest {

    // These tests needs a protocol version to (de)serialize INITCONDs. Since we're using basic types, I don't think the binary format
    // is going to change at any time in the future, so the actual version does not matter.
    private static final ProtocolVersion PROTOCOL_VERSION = ProtocolVersion.NEWEST_SUPPORTED;
    private CodecRegistry codecRegistry = new CodecRegistry();

    KeyspaceMetadata keyspace;

    @BeforeMethod(groups = "unit")
    public void setup() {
        keyspace = new KeyspaceMetadata("ks", false, Collections.<String, String>emptyMap());
    }

    @Test(groups = "unit")
    public void should_use_all_fields_in_equals_and_hashCode() {
        EqualsVerifier.forClass(AggregateMetadata.class)
            .allFieldsShouldBeUsedExcept("simpleName", "finalFuncSimpleName", "stateFuncSimpleName", "stateTypeCodec")
            .verify();
    }

    @Test(groups = "unit")
    public void should_parse_and_format_aggregate_with_initcond_and_no_finalfunc() {
        FunctionMetadata stateFunc = mock(FunctionMetadata.class);
        keyspace.functions.put("cat(text,int)", stateFunc);

        AggregateMetadata aggregate = AggregateMetadata.build(keyspace, SYSTEM_ROW_CAT_TOS, PROTOCOL_VERSION, codecRegistry);

        assertThat(aggregate).isNotNull();
        assertThat(aggregate.getFullName()).isEqualTo("cat_tos(int)");
        assertThat(aggregate.getSimpleName()).isEqualTo("cat_tos");
        assertThat(aggregate.getArgumentTypes()).containsExactly(DataType.cint());
        assertThat(aggregate.getFinalFunc()).isNull();
        assertThat(aggregate.getInitCond()).isEqualTo("0");
        assertThat(aggregate.getReturnType()).isEqualTo(DataType.text());
        assertThat(aggregate.getStateFunc()).isEqualTo(stateFunc);
        assertThat(aggregate.getStateType()).isEqualTo(DataType.text());

        assertThat(keyspace.getAggregate("cat_tos", DataType.cint())).isEqualTo(aggregate);

        assertThat(aggregate.toString()).isEqualTo("CREATE AGGREGATE ks.cat_tos(int) SFUNC cat STYPE text INITCOND '0';");

        assertThat(aggregate.exportAsString()).isEqualTo("CREATE AGGREGATE ks.cat_tos(int)\n"
            + "SFUNC cat STYPE text\n"
            + "INITCOND '0';");
    }

    @Test(groups = "unit")
    public void should_parse_and_format_aggregate_with_no_arguments() {
        FunctionMetadata stateFunc = mock(FunctionMetadata.class);
        keyspace.functions.put("inc(int)", stateFunc);

        AggregateMetadata aggregate = AggregateMetadata.build(keyspace, SYSTEM_ROW_MYCOUNT, PROTOCOL_VERSION, codecRegistry);

        assertThat(aggregate).isNotNull();
        assertThat(aggregate.getFullName()).isEqualTo("mycount()");
        assertThat(aggregate.getSimpleName()).isEqualTo("mycount");
        assertThat(aggregate.getArgumentTypes()).isEmpty();
        assertThat(aggregate.getFinalFunc()).isNull();
        assertThat(aggregate.getInitCond()).isEqualTo(0);
        assertThat(aggregate.getReturnType()).isEqualTo(DataType.cint());
        assertThat(aggregate.getStateFunc()).isEqualTo(stateFunc);
        assertThat(aggregate.getStateType()).isEqualTo(DataType.cint());

        assertThat(keyspace.getAggregate("mycount")).isEqualTo(aggregate);

        assertThat(aggregate.toString()).isEqualTo("CREATE AGGREGATE ks.mycount() SFUNC inc STYPE int INITCOND 0;");

        assertThat(aggregate.exportAsString()).isEqualTo("CREATE AGGREGATE ks.mycount()\n"
            + "SFUNC inc STYPE int\n"
            + "INITCOND 0;");
    }

    @Test(groups = "unit")
    public void should_parse_and_format_aggregate_with_final_function() {
        FunctionMetadata stateFunc = mock(FunctionMetadata.class);
        keyspace.functions.put("plus(int,int)", stateFunc);
        FunctionMetadata finalFunc = mock(FunctionMetadata.class);
        keyspace.functions.put("announce(int)", finalFunc);

        AggregateMetadata aggregate = AggregateMetadata.build(keyspace, SYSTEM_ROW_PRETTYSUM, PROTOCOL_VERSION, codecRegistry);

        assertThat(aggregate).isNotNull();
        assertThat(aggregate.getFullName()).isEqualTo("prettysum(int)");
        assertThat(aggregate.getSimpleName()).isEqualTo("prettysum");
        assertThat(aggregate.getArgumentTypes()).containsExactly(DataType.cint());
        assertThat(aggregate.getFinalFunc()).isEqualTo(finalFunc);
        assertThat(aggregate.getInitCond()).isEqualTo(0);
        assertThat(aggregate.getReturnType()).isEqualTo(DataType.text());
        assertThat(aggregate.getStateFunc()).isEqualTo(stateFunc);
        assertThat(aggregate.getStateType()).isEqualTo(DataType.cint());

        assertThat(keyspace.getAggregate("prettysum", DataType.cint())).isEqualTo(aggregate);

        assertThat(aggregate.toString()).isEqualTo("CREATE AGGREGATE ks.prettysum(int) SFUNC plus STYPE int FINALFUNC announce INITCOND 0;");

        assertThat(aggregate.exportAsString()).isEqualTo("CREATE AGGREGATE ks.prettysum(int)\n"
            + "SFUNC plus STYPE int\n"
            + "FINALFUNC announce\n"
            + "INITCOND 0;");
    }

    @Test(groups = "unit")
    public void should_parse_and_format_aggregate_with_no_initcond() {
        FunctionMetadata stateFunc = mock(FunctionMetadata.class);
        keyspace.functions.put("plus(int,int)", stateFunc);

        AggregateMetadata aggregate = AggregateMetadata.build(keyspace, SYSTEM_ROW_SUM, PROTOCOL_VERSION, codecRegistry);

        assertThat(aggregate).isNotNull();
        assertThat(aggregate.getFullName()).isEqualTo("sum(int)");
        assertThat(aggregate.getSimpleName()).isEqualTo("sum");
        assertThat(aggregate.getArgumentTypes()).containsExactly(DataType.cint());
        assertThat(aggregate.getFinalFunc()).isNull();
        assertThat(aggregate.getInitCond()).isNull();
        assertThat(aggregate.getReturnType()).isEqualTo(DataType.cint());
        assertThat(aggregate.getStateFunc()).isEqualTo(stateFunc);
        assertThat(aggregate.getStateType()).isEqualTo(DataType.cint());

        assertThat(keyspace.getAggregate("sum", DataType.cint())).isEqualTo(aggregate);

        assertThat(aggregate.toString()).isEqualTo("CREATE AGGREGATE ks.sum(int) SFUNC plus STYPE int;");

        assertThat(aggregate.exportAsString()).isEqualTo("CREATE AGGREGATE ks.sum(int)\n"
            + "SFUNC plus STYPE int;");
    }

    private static final String INT = "org.apache.cassandra.db.marshal.Int32Type";
    private static final String TEXT = "org.apache.cassandra.db.marshal.UTF8Type";

    private static final Row SYSTEM_ROW_CAT_TOS = mockRow("cat_tos", ImmutableList.of("int"), ImmutableList.of(INT),
        null, TypeCodec.varchar().serialize("0", PROTOCOL_VERSION), TEXT, "cat", TEXT);

    private static final Row SYSTEM_ROW_MYCOUNT = mockRow("mycount", Collections.<String>emptyList(), Collections.<String>emptyList(),
        null, TypeCodec.cint().serialize(0, PROTOCOL_VERSION), INT, "inc", INT);

    private static final Row SYSTEM_ROW_PRETTYSUM = mockRow("prettysum", ImmutableList.of("int"), ImmutableList.of(INT),
        "announce", TypeCodec.cint().serialize(0, PROTOCOL_VERSION), TEXT, "plus", INT);

    private static final Row SYSTEM_ROW_SUM = mockRow("sum", ImmutableList.of("int"), ImmutableList.of(INT),
        null, null, INT, "plus", INT);

    private static Row mockRow(String aggregateName, List<String> signature, List<String> argumentTypes, String finalFunc,
                               ByteBuffer initCond, String returnType, String stateFunc, String stateType) {
        Row row = mock(Row.class);

        when(row.getString("aggregate_name")).thenReturn(aggregateName);
        when(row.getList("signature", String.class)).thenReturn(signature);
        when(row.getList("argument_types", String.class)).thenReturn(argumentTypes);
        when(row.getString("final_func")).thenReturn(finalFunc);
        when(row.getBytes("initcond")).thenReturn(initCond);
        when(row.getString("return_type")).thenReturn(returnType);
        when(row.getString("state_func")).thenReturn(stateFunc);
        when(row.getString("state_type")).thenReturn(stateType);

        return row;
    }
}
