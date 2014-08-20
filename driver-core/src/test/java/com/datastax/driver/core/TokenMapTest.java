package com.datastax.driver.core;

import java.nio.ByteBuffer;
import java.util.Set;

import org.testng.annotations.*;

import static org.testng.Assert.assertFalse;

import com.datastax.driver.core.CCMBridge.CCMCluster;

/**
 * Tests that the token map is correctly initialized at startup (JAVA-415).
 */
public class TokenMapTest {
    CCMCluster ccmCluster = null;
    Cluster cluster = null;

    @BeforeMethod(groups = "short")
    public void setup() {
        ccmCluster = CCMBridge.buildCluster(1, Cluster.builder());
        cluster = ccmCluster.cluster;
    }

    @Test(groups = "short")
    public void initTest() {
        // If the token map is initialized correctly, we should get replicas for any partition key
        ByteBuffer anyKey = ByteBuffer.wrap(new byte[]{});
        Set<Host> replicas = cluster.getMetadata().getReplicas("system", anyKey);
        assertFalse(replicas.isEmpty());
    }

    @AfterMethod(groups = "short")
    public void teardown() {
        ccmCluster.discard();
    }
}
