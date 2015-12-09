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

import com.datastax.driver.core.CCMBridge;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.utils.CassandraVersion;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.datastax.driver.core.DataType.cint;
import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

@CassandraVersion(major = 2.1, minor = 3)
public class QueryBuilderTupleExecutionTest extends CCMBridge.PerClassSingleNodeCluster {

    @Override
    protected Collection<String> getTableDefinitions() {
        return Collections.emptyList();
    }

    @Test(groups = "short")
    public void should_handle_tuple() throws Exception {
        String query = "INSERT INTO foo (k,x) VALUES (0,(1));";
        TupleType tupleType = TupleType.of(cint());
        BuiltStatement insert = insertInto("foo").value("k", 0).value("x", tupleType.newValue(1));
        assertEquals(insert.toString(), query);
    }

    @SuppressWarnings("deprecation")
    @Test(groups = "short")
    public void should_handle_collections_of_tuples() {
        String query;
        BuiltStatement statement;
        query = "UPDATE foo SET l=[(1, 2)] WHERE k=1;";
        TupleType tupleType = TupleType.of(cint(), cint());
        List<TupleValue> list = ImmutableList.of(tupleType.newValue(1, 2));
        statement = update("foo").with(set("l", list)).where(eq("k", 1));
        assertThat(statement.toString()).isEqualTo(query);
    }

}
