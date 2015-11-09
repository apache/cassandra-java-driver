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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.utils.CassandraVersion;

public class SimpleStatementIntegrationTest extends CCMBridge.PerClassSingleNodeCluster {
    @Override
    protected Collection<String> getTableDefinitions() {
        return Lists.newArrayList(
            "CREATE TABLE foo (k int primary key, v1 int, v2 int)"
        );
    }

    @BeforeMethod(groups = "short")
    public void setup() {
        session.execute("TRUNCATE foo");
    }

    @Test(groups = "short")
    @CassandraVersion(major = 2.1)
    public void should_allow_unset_positional_values_if_last_one_is_set() {
        // Given
        SimpleStatement statement = session.newSimpleStatement("INSERT INTO foo (k, v1, v2) VALUES (?, ?, ?)");
        statement.setInt(0, 1);
        statement.setInt(2, 1);

        // When
        session.execute(statement);
        Row row = session.execute("SELECT * FROM foo WHERE k = 1").one();

        // Then
        assertThat(row.getInt("k")).isEqualTo(1);
        assertThat(row.isNull("v1")).isTrue();
        assertThat(row.getInt("v2")).isEqualTo(1);
    }
}
