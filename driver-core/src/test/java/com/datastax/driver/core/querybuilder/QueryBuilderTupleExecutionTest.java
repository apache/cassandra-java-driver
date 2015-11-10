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
package com.datastax.driver.core.querybuilder;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

import com.datastax.driver.core.CCMBridge;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.utils.CassandraVersion;

import static com.datastax.driver.core.DataType.cint;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;

@CassandraVersion(major=2.1, minor=3)
public class QueryBuilderTupleExecutionTest extends CCMBridge.PerClassSingleNodeCluster {

    @Override
    protected Collection<String> getTableDefinitions() {
            return Collections.emptyList();
    }

    @Test(groups = "short")
    public void should_handle_tuple() throws Exception {
        String query = "INSERT INTO foo (k,x) VALUES (0,(1));";
        TupleType tupleType = cluster.getMetadata().newTupleType(cint());
        BuiltStatement insert = new QueryBuilder(cluster).insertInto("foo").value("k", 0).value("x", tupleType.newValue(1));
        assertEquals(insert.toString(), query);
    }

    @SuppressWarnings("deprecation")
    @Test(groups = "short")
    public void should_handle_collections_of_tuples() {
        String query;
        BuiltStatement statement;
        query = "UPDATE foo SET l=[(1, 2)] WHERE k=1;";
        TupleType tupleType = cluster.getMetadata().newTupleType(cint(), cint());
        List<TupleValue> list = ImmutableList.of(tupleType.newValue(1, 2));
        statement = new QueryBuilder(cluster).update("foo").with(set("l", list)).where(eq("k", 1));
        assertThat(statement.toString()).isEqualTo(query);
    }

}
