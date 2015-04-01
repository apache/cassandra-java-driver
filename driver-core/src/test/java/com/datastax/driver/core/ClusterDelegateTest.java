package com.datastax.driver.core;

import java.util.Collection;

import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ClusterDelegateTest extends CCMBridge.PerClassSingleNodeCluster {
    @Override
    protected Collection<String> getTableDefinitions() {
        return Lists.newArrayList();
    }

    @Test(groups = "short")
    public void should_allow_subclass_to_delegate_to_other_instance() {
        SimpleDelegatingCluster delegatingCluster = new SimpleDelegatingCluster(cluster);

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
