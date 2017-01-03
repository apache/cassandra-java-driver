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

import org.testng.annotations.Test;

/**
 * Tests for {@link Cluster}.
 *
 * @since 3.2
 */
public class ClusterTest {

    /**
     * <p>
     * Validates that we cannot add a contact point it it cannot be resolved.
     * </p>
     *
     * @test_category cluster
     * @expected_result A failure is thrown when the contact point cannot be resolved.
     */
    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void should_fail_if_cannot_resolve_address() {
        Cluster.builder().addContactPoint("foo");
    }

    /**
     * <p>
     * Validates that we cannot add contact points if one cannot be resolved.
     * </p>
     *
     * @test_category cluster
     * @expected_result A failure is thrown when a contact point cannot be resolved.
     */
    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void should_fail_if_cannot_resolve_only_one_address() {
        Cluster.builder().addContactPoints("127.0.0.1", "foo");
    }

    /**
     * <p>
     * Validates that we can add contact points even if we have addresses that cannot be resolved.
     * </p>
     *
     * @test_category cluster
     * @expected_result No failure is thrown as we have one contact point that can be resolved.
     * @jira_ticket JAVA-1334
     */
    @Test(groups = "unit")
    public void should_not_fail_if_cannot_resolve_all_addresses() {
        Cluster.builder().addKnownContactPointsOnly("127.0.0.1", "foo");
    }

    /**
     * <p>
     * Validates that we cannot add contact points if none can be resolved.
     * </p>
     *
     * @test_category cluster
     * @expected_result A failure is thrown as no contact points can be resolved.
     * @jira_ticket JAVA-1334
     */
    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void should_fail_if_cannot_resolve_any_addresses() {
        Cluster.builder().addKnownContactPointsOnly("foo", "bar");
    }
}
