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

import java.util.List;

import org.testng.annotations.Test;

import com.datastax.driver.core.*;
import com.datastax.driver.core.RegularStatement.ValueDefinition;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;

public class BatchTest {
    QueryBuilder builder = new QueryBuilder(ProtocolVersion.V4, CodecRegistry.DEFAULT_INSTANCE);

    @Test(groups = "unit")
    public void should_collect_values_of_built_statements_when_no_placeholders() {
        // Given
        Insert insert1 = builder.insertInto("foo").value("k", "key1");
        Insert insert2 = builder.insertInto("foo").value("k", "key2");

        // When
        Batch batch = builder.batch(insert1, insert2);

        // Then
        assertThat(batch.getQueryString()).isEqualTo("BEGIN BATCH "
            + "INSERT INTO foo (k) VALUES (?); "
            + "INSERT INTO foo (k) VALUES (?); "
            + "APPLY BATCH;");
        List<ValueDefinition> definitions = batch.getValueDefinitions();
        assertThat(definitions).hasSize(2);
        assertThat(definitions.get(0)).hasIndex(0).hasType(DataType.text());
        assertThat(definitions.get(1)).hasIndex(1).hasType(DataType.text());
        assertThat(batch.getValueDefinitions()).hasSize(2);
        assertThat(batch.getString(0)).isEqualTo("key1");
        assertThat(batch.getString(1)).isEqualTo("key2");
    }

    @Test(groups = "unit")
    public void should_inline_values_of_built_statements_when_placeholders() {
        // Given
        Insert insert1 = builder.insertInto("foo").value("k", "key1");
        Insert insert2 = builder.insertInto("foo").value("k", bindMarker());

        // When
        Batch batch = builder.batch(insert1, insert2);

        // Then
        assertThat(batch.getQueryString()).isEqualTo("BEGIN BATCH "
            + "INSERT INTO foo (k) VALUES ('key1'); "
            + "INSERT INTO foo (k) VALUES (?); "
            + "APPLY BATCH;");
        assertThat(batch.getValueDefinitions()).hasSize(0);
    }

    @Test(groups = "unit")
    public void should_inline_values_of_built_statements_when_there_are_simple_statements() {
        // Given
        Insert insert1 = builder.insertInto("foo").value("k", "key1");
        SimpleStatement insert2 = newSimpleStatement("INSERT INTO foo (k) VALUES ('key2')");

        // When
        Batch batch = builder.batch(insert1, insert2);

        // Then
        assertThat(batch.getQueryString()).isEqualTo("BEGIN BATCH "
            + "INSERT INTO foo (k) VALUES ('key1'); "
            + "INSERT INTO foo (k) VALUES ('key2'); "
            + "APPLY BATCH;");
        assertThat(batch.getValueDefinitions()).hasSize(0);
    }

    @Test(groups = "unit")
    public void should_collect_values_of_simple_statements() {
        // Given
        Insert insert1 = builder.insertInto("foo").value("k", "key1");
        SimpleStatement insert2 = newSimpleStatement("INSERT INTO foo (k) VALUES (?)", "key2");

        // When
        Batch batch = builder.batch(insert1, insert2);

        // Then
        assertThat(batch.getString(0)).isEqualTo("key2");
        assertThat(batch.getQueryString()).isEqualTo("BEGIN BATCH "
            + "INSERT INTO foo (k) VALUES ('key1'); "
            + "INSERT INTO foo (k) VALUES (?); "
            + "APPLY BATCH;");
        List<ValueDefinition> definitions = batch.getValueDefinitions();
        assertThat(definitions).hasSize(1);
        assertThat(definitions.get(0)).hasIndex(0).hasType(DataType.text());
        assertThat(batch.getString(0)).isEqualTo("key2");
    }

    @Test(groups = "unit")
    public void should_renumber_indices_when_multiple_simple_statements_with_values() {
        // Given
        SimpleStatement insert1 = newSimpleStatement("INSERT INTO foo (k) VALUES (?)", "key1");
        SimpleStatement insert2 = newSimpleStatement("INSERT INTO foo (k) VALUES (?)", "key2");

        // When
        Batch batch = builder.batch(insert1, insert2);

        // Then
        assertThat(batch.getQueryString()).isEqualTo("BEGIN BATCH "
            + "INSERT INTO foo (k) VALUES (?); "
            + "INSERT INTO foo (k) VALUES (?); "
            + "APPLY BATCH;");
        assertThat(batch.getValueDefinitions()).hasSize(2);
        assertThat(batch.getString(0)).isEqualTo("key1");
        assertThat(batch.getString(1)).isEqualTo("key2");
    }

    @Test(groups = "unit")
    public void should_preserve_gaps_when_renumbering_indices_of_simple_statements() {
        // Given
        SimpleStatement insert1 = newSimpleStatement("INSERT INTO foo (k, v1, v2) VALUES (?, ?, ?)");
        insert1.setString(0, "k in insert1");
        insert1.setString(2, "v2 in insert1");
        SimpleStatement insert2 = newSimpleStatement("INSERT INTO foo (k, v1, v2) VALUES (?, ?, ?)");
        insert2.setString(1, "v1 in insert2");
        insert2.setString(2, "v2 in insert2");

        // When
        Batch batch = builder.batch(insert1, insert2);

        // Then
        assertThat(batch.getQueryString()).isEqualTo("BEGIN BATCH "
            + "INSERT INTO foo (k, v1, v2) VALUES (?, ?, ?); "
            + "INSERT INTO foo (k, v1, v2) VALUES (?, ?, ?); "
            + "APPLY BATCH;");
        assertThat(batch.getValueDefinitions()).hasSize(4);
        assertThat(batch.getString(0)).isEqualTo("k in insert1");
        assertThat(batch.getString(2)).isEqualTo("v2 in insert1");
        assertThat(batch.getString(4)).isEqualTo("v1 in insert2");
        assertThat(batch.getString(5)).isEqualTo("v2 in insert2");
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void should_refuse_simple_statements_with_named_values() {
        // Given
        SimpleStatement insert1 = newSimpleStatement("INSERT INTO foo (k) VALUES (:key)");
        insert1.setString("key", "key1");

        // When
        Batch batch = builder.batch(insert1);

        // Then
        // an exception is thrown
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void should_fail_if_too_many_values_from_simple_statements() {
        // Given
        SimpleStatement insert1 = newSimpleStatement("INSERT INTO foo (k) VALUES (?)", "key1");

        // When
        Batch batch = builder.batch();
        for (int i = 0; i < 65536; i++)
            batch.add(insert1);

        // Then
        batch.getValueDefinitions(); // triggers a cache refresh which should throw an exception
    }

    private static SimpleStatement newSimpleStatement(String query, Object... values) {
        return CoreHooks.newSimpleStatement(query, ProtocolVersion.V4, CodecRegistry.DEFAULT_INSTANCE, values);
    }
}
