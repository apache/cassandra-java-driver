package com.datastax.driver.core.policies;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import com.datastax.driver.core.CCMBridge;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;

public class DCAwareRoundRobinPolicyTest {

    @Test(groups = "short")
    public void should_pick_local_dc_from_contact_points() {
        CCMBridge ccm = null;
        Cluster cluster = null;
        DCAwareRoundRobinPolicy policy;

        try {
            // Create two-DC cluster with one host in each DC
            ccm = CCMBridge.create("test", 1, 1);
            policy = new DCAwareRoundRobinPolicy();

            // Pass host1 as contact point
            cluster = Cluster.builder()
                             .addContactPoint(CCMBridge.ipOfNode(1))
                             .withLoadBalancingPolicy(policy)
                             .build();
            cluster.init();

            // Policy's localDC should be the one from the contact point
            Host host1 = findHost(cluster, CCMBridge.ipOfNode(1));
            assertEquals(policy.localDc, host1.getDatacenter());

        } finally {
            if (cluster != null)
                cluster.close();
            if (ccm != null)
                ccm.remove();
        }
    }

    private static Host findHost(Cluster cluster, String ip) {
        for (Host host : cluster.getMetadata().getAllHosts()) {
            if (host.getAddress().getHostAddress().equals(ip))
                return host;
        }
        fail(ip + " not found in cluster metadata");
        return null;
    }
}
