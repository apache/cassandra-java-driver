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

import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.UnsupportedFeatureException;
import com.datastax.driver.core.utils.CassandraVersion;
import com.google.common.collect.ImmutableMap;
import org.testng.SkipException;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SimpleStatementIntegrationTest extends CCMTestsSupport {

    @Override
    public void onTestContextInitialized() {
        execute(
                "CREATE TABLE users(id int, id2 int, name text, primary key (id, id2))",
                "INSERT INTO users(id, id2, name) VALUES (1, 2, 'test')"
        );
    }

    @Test(groups = "short")
    @CassandraVersion("2.1.0")
    public void should_execute_query_with_named_values() {
        // Given
        SimpleStatement statement = new SimpleStatement("SELECT * FROM users WHERE id = :id and id2 = :id2",
                ImmutableMap.<String, Object>of("id", 1, "id2", 2));

        // When
        Row row = session().execute(statement).one();

        // Then
        assertThat(row).isNotNull();
        assertThat(row.getString("name")).isEqualTo("test");
    }

    @Test(groups = "short", expectedExceptions = InvalidQueryException.class)
    @CassandraVersion("2.1.0")
    public void should_fail_if_query_with_named_values_but_missing_parameter() {
        // Given a Statement missing named parameters.
        SimpleStatement statement = new SimpleStatement("SELECT * FROM users WHERE id = :id and id2 = :id2",
                ImmutableMap.<String, Object>of("id2", 2));

        // When
        session().execute(statement).one();

        // Then - The driver does allow this because it doesn't know what parameters are required, but C* should
        //        throw an InvalidQueryException.
    }

    @Test(groups = "short", expectedExceptions = InvalidQueryException.class)
    @CassandraVersion("2.1.0")
    public void should_fail_if_query_with_named_values_but_using_wrong_type() {
        // Given a Statement using a named parameter with the wrong value for the type (id is of type int, using double)
        SimpleStatement statement = new SimpleStatement("SELECT * FROM users WHERE id = :id and id2 = :id2",
                ImmutableMap.<String, Object>of("id", 2.7, "id2", 2));

        // When
        session().execute(statement).one();

        // Then - The driver does allow this because it doesn't know the type information, but C* should throw an
        //        InvalidQueryException.
    }

    public void useNamedValuesWithProtocol(ProtocolVersion version) {
        Cluster vCluster = createClusterBuilder()
                .addContactPoints(getContactPoints())
                .withPort(ccm().getBinaryPort())
                .withProtocolVersion(version).build();
        try {
            Session vSession = vCluster.connect(this.keyspace);
            // Given - A simple statement with named parameters.
            SimpleStatement statement = new SimpleStatement("SELECT * FROM users WHERE id = :id",
                    ImmutableMap.<String, Object>of("id", 1));

            // When - Executing that statement against a Cluster instance using Protocol Version V2.
            vSession.execute(statement).one();

            // Then - Should throw an UnsupportedFeatureException
        } finally {
            vCluster.close();
        }
    }

    @Test(groups = "short", expectedExceptions = UnsupportedFeatureException.class)
    @CassandraVersion("2.0.0")
    public void should_fail_if_query_with_named_values_if_protocol_is_V2() {
        if (ccm().getCassandraVersion().getMajor() >= 3) {
            throw new SkipException("Skipping since Cassandra 3.0+ does not support protocol v2");
        }
        useNamedValuesWithProtocol(ProtocolVersion.V2);
    }

    @Test(groups = "short", expectedExceptions = UnsupportedFeatureException.class)
    @CassandraVersion("2.0.0")
    public void should_fail_if_query_with_named_values_if_protocol_is_V1() {
        if (ccm().getCassandraVersion().getMajor() >= 3) {
            throw new SkipException("Skipping since Cassandra 3.0+ does not support protocol v1");
        }
        useNamedValuesWithProtocol(ProtocolVersion.V1);
    }
}
