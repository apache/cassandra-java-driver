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

import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DelegatingClusterIntegrationTest extends CCMTestsSupport {

    @Test(groups = "short")
    public void should_allow_subclass_to_delegate_to_other_instance() {
        SimpleDelegatingCluster delegatingCluster = new SimpleDelegatingCluster(cluster());

        ResultSet rs = delegatingCluster.connect().execute("select * from system.local");

        assertThat(rs.all()).hasSize(1);
    }

    static class SimpleDelegatingCluster extends DelegatingCluster {

        private final Cluster delegate;

        public SimpleDelegatingCluster(Cluster delegate) {
            this.delegate = delegate;
        }

        @Override
        protected Cluster delegate() {
            return delegate;
        }
    }
}
