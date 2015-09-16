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

import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.DataType.*;
import static com.datastax.driver.core.DataTypeParser.parse;
public class DataTypeParserTest extends CCMBridge.PerClassSingleNodeCluster {

    @Test(groups = "short")
    public void should_parse_native_types() {
        assertThat(parse("ascii", cluster.getMetadata())).isEqualTo(ascii());
        assertThat(parse("bigint", cluster.getMetadata())).isEqualTo(bigint());
        assertThat(parse("blob", cluster.getMetadata())).isEqualTo(blob());
        assertThat(parse("boolean", cluster.getMetadata())).isEqualTo(cboolean());
        assertThat(parse("counter", cluster.getMetadata())).isEqualTo(counter());
        assertThat(parse("decimal", cluster.getMetadata())).isEqualTo(decimal());
        assertThat(parse("double", cluster.getMetadata())).isEqualTo(cdouble());
        assertThat(parse("float", cluster.getMetadata())).isEqualTo(cfloat());
        assertThat(parse("inet", cluster.getMetadata())).isEqualTo(inet());
        assertThat(parse("int", cluster.getMetadata())).isEqualTo(cint());
        assertThat(parse("text", cluster.getMetadata())).isEqualTo(text());
        assertThat(parse("varchar", cluster.getMetadata())).isEqualTo(varchar());
        assertThat(parse("timestamp", cluster.getMetadata())).isEqualTo(timestamp());
        assertThat(parse("date", cluster.getMetadata())).isEqualTo(date());
        assertThat(parse("time", cluster.getMetadata())).isEqualTo(time());
        assertThat(parse("uuid", cluster.getMetadata())).isEqualTo(uuid());
        assertThat(parse("varint", cluster.getMetadata())).isEqualTo(varint());
        assertThat(parse("timeuuid", cluster.getMetadata())).isEqualTo(timeuuid());
        assertThat(parse("tinyint", cluster.getMetadata())).isEqualTo(tinyint());
        assertThat(parse("smallint", cluster.getMetadata())).isEqualTo(smallint());
    }

    @Test(groups = "short")
    public void should_ignore_whitespace() {
        assertThat(parse("  int  ", cluster.getMetadata())).isEqualTo(cint());
        assertThat(parse("  set < bigint > ", cluster.getMetadata())).isEqualTo(set(bigint()));
        assertThat(parse("  map  <  date  ,  timeuuid  >  ", cluster.getMetadata())).isEqualTo(map(date(), timeuuid()));
    }

    @Test(groups = "short")
    public void should_parse_collection_types() {
        assertThat(parse("list<int>", cluster.getMetadata())).isEqualTo(list(cint()));
        assertThat(parse("set<bigint>", cluster.getMetadata())).isEqualTo(set(bigint()));
        assertThat(parse("map<date,timeuuid>", cluster.getMetadata())).isEqualTo(map(date(), timeuuid()));
    }

    @Test(groups = "short")
    public void should_parse_frozen_collection_types() {
        assertThat(parse("frozen<list<int>>", cluster.getMetadata())).isEqualTo(list(cint(), true));
        assertThat(parse("frozen<set<bigint>>", cluster.getMetadata())).isEqualTo(set(bigint(), true));
        assertThat(parse("frozen<map<date,timeuuid>>", cluster.getMetadata())).isEqualTo(map(date(), timeuuid(), true));
    }

    @Test(groups = "short")
    public void should_parse_nested_collection_types() {
        assertThat(parse("list<list<int>>", cluster.getMetadata())).isEqualTo(list(list(cint())));
        assertThat(parse("set<list<frozen<map<bigint,varchar>>>>", cluster.getMetadata())).isEqualTo(set(list(map(bigint(), varchar(), true))));
    }

    @Test(groups = "short")
    public void should_parse_tuple_types() {
        assertThat(parse("tuple<int,list<text>>", cluster.getMetadata())).isEqualTo(cluster.getMetadata().newTupleType(cint(), list(text())));
    }

    @Test(groups = "short", enabled = false)
    public void should_parse_user_defined_types() {
        assertThat(parse(keyspace + ".udt1", cluster.getMetadata())).isEqualTo(cluster.getMetadata().getKeyspace(keyspace).getUserType("udt1"));
    }

    @Override
    protected Collection<String> getTableDefinitions() {
        return Lists.newArrayList(String.format("CREATE TYPE %s.udt1 (f1 int, f2 frozen<list<text>>)", keyspace));
    }
    
}
