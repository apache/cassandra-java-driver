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
import static com.datastax.driver.core.DataType.*;
import static com.datastax.driver.core.DataTypeCqlNameParser.parse;
import static com.datastax.driver.core.Metadata.quote;

@CassandraVersion("3.0")
public class DataTypeCqlNameParserTest extends CCMTestsSupport {

    @Test(groups = "short")
    public void should_parse_native_types() {
        assertThat(parse("ascii", cluster(), null, null, null, false, false)).isEqualTo(ascii());
        assertThat(parse("bigint", cluster(), null, null, null, false, false)).isEqualTo(bigint());
        assertThat(parse("blob", cluster(), null, null, null, false, false)).isEqualTo(blob());
        assertThat(parse("boolean", cluster(), null, null, null, false, false)).isEqualTo(cboolean());
        assertThat(parse("counter", cluster(), null, null, null, false, false)).isEqualTo(counter());
        assertThat(parse("decimal", cluster(), null, null, null, false, false)).isEqualTo(decimal());
        assertThat(parse("double", cluster(), null, null, null, false, false)).isEqualTo(cdouble());
        assertThat(parse("float", cluster(), null, null, null, false, false)).isEqualTo(cfloat());
        assertThat(parse("inet", cluster(), null, null, null, false, false)).isEqualTo(inet());
        assertThat(parse("int", cluster(), null, null, null, false, false)).isEqualTo(cint());
        assertThat(parse("text", cluster(), null, null, null, false, false)).isEqualTo(text());
        assertThat(parse("varchar", cluster(), null, null, null, false, false)).isEqualTo(varchar());
        assertThat(parse("timestamp", cluster(), null, null, null, false, false)).isEqualTo(timestamp());
        assertThat(parse("date", cluster(), null, null, null, false, false)).isEqualTo(date());
        assertThat(parse("time", cluster(), null, null, null, false, false)).isEqualTo(time());
        assertThat(parse("uuid", cluster(), null, null, null, false, false)).isEqualTo(uuid());
        assertThat(parse("varint", cluster(), null, null, null, false, false)).isEqualTo(varint());
        assertThat(parse("timeuuid", cluster(), null, null, null, false, false)).isEqualTo(timeuuid());
        assertThat(parse("tinyint", cluster(), null, null, null, false, false)).isEqualTo(tinyint());
        assertThat(parse("smallint", cluster(), null, null, null, false, false)).isEqualTo(smallint());
    }

    @Test(groups = "short")
    public void should_ignore_whitespace() {
        assertThat(parse("  int  ", cluster(), null, null, null, false, false)).isEqualTo(cint());
        assertThat(parse("  set < bigint > ", cluster(), null, null, null, false, false)).isEqualTo(set(bigint()));
        assertThat(parse("  map  <  date  ,  timeuuid  >  ", cluster(), null, null, null, false, false)).isEqualTo(map(date(), timeuuid()));
    }

    @Test(groups = "short")
    public void should_ignore_case() {
        assertThat(parse("INT", cluster(), null, null, null, false, false)).isEqualTo(cint());
        assertThat(parse("SET<BIGint>", cluster(), null, null, null, false, false)).isEqualTo(set(bigint()));
        assertThat(parse("FROZEN<mAp<Date,Tuple<timeUUID>>>", cluster(), null, null, null, false, false)).isEqualTo(map(date(), cluster().getMetadata().newTupleType(timeuuid()), true));
    }

    @Test(groups = "short")
    public void should_parse_collection_types() {
        assertThat(parse("list<int>", cluster(), null, null, null, false, false)).isEqualTo(list(cint()));
        assertThat(parse("set<bigint>", cluster(), null, null, null, false, false)).isEqualTo(set(bigint()));
        assertThat(parse("map<date,timeuuid>", cluster(), null, null, null, false, false)).isEqualTo(map(date(), timeuuid()));
    }

    @Test(groups = "short")
    public void should_parse_frozen_collection_types() {
        assertThat(parse("frozen<list<int>>", cluster(), null, null, null, false, false)).isEqualTo(list(cint(), true));
        assertThat(parse("frozen<set<bigint>>", cluster(), null, null, null, false, false)).isEqualTo(set(bigint(), true));
        assertThat(parse("frozen<map<date,timeuuid>>", cluster(), null, null, null, false, false)).isEqualTo(map(date(), timeuuid(), true));
    }

    @Test(groups = "short")
    public void should_parse_nested_collection_types() {
        Metadata metadata = cluster().getMetadata();
        KeyspaceMetadata keyspaceMetadata = metadata.getKeyspace(this.keyspace);
        assertThat(parse("list<list<int>>", cluster(), null, null, null, false, false)).isEqualTo(list(list(cint())));
        assertThat(parse("set<list<frozen<map<bigint,varchar>>>>", cluster(), null, null, null, false, false)).isEqualTo(set(list(map(bigint(), varchar(), true))));

        UserType keyType = keyspaceMetadata.getUserType(quote("Incr,edibly\" EvilTy<>><<><p\"e"));
        UserType valueType = keyspaceMetadata.getUserType(quote("A"));
        assertThat(parse("map<frozen<\"Incr,edibly\"\" EvilTy<>><<><p\"\"e\">,frozen<\"A\">>", cluster(), keyspace, keyspaceMetadata.userTypes, null, false, false))
                .isEqualTo(map(keyType, valueType, false));
    }

    @Test(groups = "short")
    public void should_parse_tuple_types() {
        assertThat(parse("tuple<int,list<text>>", cluster(), null, null, null, false, false)).isEqualTo(cluster().getMetadata().newTupleType(cint(), list(text())));
    }

    @Test(groups = "short")
    public void should_parse_user_defined_type_when_definition_in_current_user_types() {
        Metadata metadata = cluster().getMetadata();
        KeyspaceMetadata keyspaceMetadata = metadata.getKeyspace(this.keyspace);
        assertThat(parse("frozen<\"A\">", cluster(), keyspace, keyspaceMetadata.userTypes, null, false, false)).isUserType(keyspace, "A");
    }

    @Test(groups = "short")
    public void should_parse_user_defined_type_when_definition_in_old_user_types() {
        Metadata metadata = cluster().getMetadata();
        KeyspaceMetadata keyspaceMetadata = metadata.getKeyspace(this.keyspace);
        assertThat(parse("\"A\"", cluster(), keyspace, null, keyspaceMetadata.userTypes, false, false)).isUserType(keyspace, "A");
    }

    @Test(groups = "short")
    public void should_parse_user_defined_type_to_shallow_type_if_requested() {
        assertThat(parse("\"A\"", cluster(), keyspace, null, null, false, true)).isShallowUserType(keyspace, "A");
    }

    @Override
    public void onTestContextInitialized() {
        execute(
                String.format("CREATE TYPE %s.\"A\" (f1 int)", keyspace),
                String.format("CREATE TYPE %s.\"Incr,edibly\"\" EvilTy<>><<><p\"\"e\" (a frozen<\"A\">)", keyspace)
        );
    }

}
