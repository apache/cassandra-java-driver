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

import static com.datastax.driver.core.Assertions.*;

public class PreparedIdTest extends CCMBridge.PerClassSingleNodeCluster {

    @Override
    protected Collection<String> getTableDefinitions() {
        return Lists.newArrayList(
            "CREATE TABLE foo(k1 int, k2 int, k3 int, v int, PRIMARY KEY ((k1, k2, k3)))"
        );
    }

    @Test(groups = "short")
    public void should_have_routing_key_indexes_when_all_bound() {
        PreparedStatement pst = session.prepare("INSERT INTO foo (k3, k1, k2, v) VALUES (?, ?, ?, ?)");
        assertThat(pst.getPreparedId().routingKeyIndexes).containsExactly(1, 2, 0);
    }

    @Test(groups = "short")
    public void should_not_have_routing_key_indexes_when_some_not_bound() {
        PreparedStatement pst = session.prepare("INSERT INTO foo (k3, k1, k2, v) VALUES (1, ?, ?, ?)");
        assertThat(pst.getPreparedId().routingKeyIndexes).isNull();
    }

    @Test(groups = "short")
    public void should_not_have_routing_key_indexes_when_none_bound() {
        PreparedStatement pst = session.prepare("INSERT INTO foo (k3, k1, k2, v) VALUES (1, 1, 1, ?)");
        assertThat(pst.getPreparedId().routingKeyIndexes).isNull();
    }
}