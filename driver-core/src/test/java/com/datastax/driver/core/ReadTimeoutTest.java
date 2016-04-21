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

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.OperationTimedOutException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.scassandra.http.client.PrimingRequest.queryBuilder;
import static org.scassandra.http.client.PrimingRequest.then;

public class ReadTimeoutTest extends ScassandraTestBase.PerClassCluster {

    String query = "SELECT foo FROM bar";

    @BeforeMethod(groups = "short")
    public void setup() {
        primingClient.prime(
                queryBuilder()
                        .withQuery(query)
                        .withThen(then().withFixedDelay(100L))
                        .build()
        );

        // Set default timeout too low
        cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(10);
    }

    @Test(groups = "short")
    public void should_use_default_timeout_if_not_overridden_by_statement() {
        try {
            session.execute(query);
            fail("expected a timeout");
        } catch (NoHostAvailableException e) {
            Throwable t = e.getErrors().values().iterator().next();
            assertThat(t).isInstanceOf(OperationTimedOutException.class);
        }
    }

    @Test(groups = "short")
    public void should_use_statement_timeout_if_overridden() {
        Statement statement = new SimpleStatement(query).setReadTimeoutMillis(200);
        session.execute(statement);
    }

    @Test(groups = "short")
    public void should_disable_timeout_if_set_to_zero_at_statement_level() {
        Statement statement = new SimpleStatement(query).setReadTimeoutMillis(0);
        session.execute(statement);
    }
}
