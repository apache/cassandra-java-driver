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

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.querybuilder.BuiltStatement;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class StatementIdempotenceTest {
    @Test(groups = "unit")
    public void should_default_to_false_when_not_set_on_statement_nor_query_options() {
        QueryOptions queryOptions = new QueryOptions();
        SimpleStatement statement = new SimpleStatement("");

        assertThat(statement.isIdempotentWithDefault(queryOptions)).isFalse();
    }

    @Test(groups = "unit")
    public void should_use_query_options_when_not_set_on_statement() {
        QueryOptions queryOptions = new QueryOptions();
        SimpleStatement statement = new SimpleStatement("");

        for (boolean valueInOptions : new boolean[]{ true, false }) {
            queryOptions.setDefaultIdempotence(valueInOptions);
            assertThat(statement.isIdempotentWithDefault(queryOptions)).isEqualTo(valueInOptions);
        }
    }

    @Test(groups = "unit")
    public void should_use_statement_when_set_on_statement() {
        QueryOptions queryOptions = new QueryOptions();
        SimpleStatement statement = new SimpleStatement("");

        for (boolean valueInOptions : new boolean[]{ true, false })
            for (boolean valueInStatement : new boolean[]{ true, false }) {
                queryOptions.setDefaultIdempotence(valueInOptions);
                statement.setIdempotent(valueInStatement);
                assertThat(statement.isIdempotentWithDefault(queryOptions)).isEqualTo(valueInStatement);
            }
    }

    @Test(groups = "unit")
    public void should_infer_for_built_statement() {
        for (BuiltStatement statement : idempotentBuiltStatements())
            assertThat(statement.isIdempotent())
                .as(statement.getQueryString())
                .isTrue();

        for (BuiltStatement statement : nonIdempotentBuiltStatements())
            assertThat(statement.isIdempotent())
                .as(statement.getQueryString())
                .isFalse();
    }

    @Test(groups = "unit")
    public void should_override_inferred_value_when_manually_set_on_built_statement() {
        for (boolean manualValue : new boolean[]{ true, false }) {
            for (BuiltStatement statement : idempotentBuiltStatements()) {
                statement.setIdempotent(manualValue);
                assertThat(statement.isIdempotent()).isEqualTo(manualValue);
            }

            for (BuiltStatement statement : nonIdempotentBuiltStatements()) {
                statement.setIdempotent(manualValue);
                assertThat(statement.isIdempotent()).isEqualTo(manualValue);
            }
        }
    }

    private static ImmutableList<BuiltStatement> idempotentBuiltStatements() {
        return ImmutableList.<BuiltStatement>of(
            update("foo").with(set("v", 1)).where(eq("k", 1)), // set simple value
            update("foo").with(add("s", 1)).where(eq("k", 1)), // add to set
            update("foo").with(put("m", "a", 1)).where(eq("k", 1)) // put in map
        );
    }

    private static ImmutableList<BuiltStatement> nonIdempotentBuiltStatements() {
        return ImmutableList.<BuiltStatement>of(
            update("foo").with(append("l", 1)).where(eq("k", 1)), // append to list
            update("foo").with(set("v", 1)).and(prepend("l", 1)).where(eq("k", 1)), // prepend to list
            update("foo").with(incr("c")).where(eq("k", 1)) // counter update
        );
    }
}
