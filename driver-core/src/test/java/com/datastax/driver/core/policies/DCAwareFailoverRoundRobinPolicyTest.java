package com.datastax.driver.core.policies;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.ScassandraCluster.datacenter;
import static com.datastax.driver.core.TestUtils.nonQuietClusterCloseOptions;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.*;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.MemoryAppender;
import com.datastax.driver.core.QueryTracker;
import com.datastax.driver.core.ScassandraCluster;
import com.datastax.driver.core.Session;

public class DCAwareFailoverRoundRobinPolicyTest {
	DCAwareFailoverRoundRobinPolicy policy;
	Collection<Host> hosts;
	Cluster cluster;

    Logger policyLogger = Logger.getLogger(DCAwareRoundRobinPolicy.class);
    MemoryAppender logs;
    QueryTracker queryTracker;

    @Captor
    ArgumentCaptor<Collection<Host>> initHostsCaptor;

    @BeforeMethod(groups = "short")
    public void setUp() throws UnknownHostException {
        initMocks(this);
        policyLogger.setLevel(Level.WARN);
        logs = new MemoryAppender();
        policyLogger.addAppender(logs);
        queryTracker = new QueryTracker();
        
        hosts = new ArrayList<Host>();
		policy = new DCAwareFailoverRoundRobinPolicy("dc1", "dc2", 2);
		for(int i=0;i<6;i++){
			Host host = Mockito.mock(Host.class);
			InetAddress address = InetAddress.getByName("127.0.0." + i);
			Mockito.when(host.getAddress()).thenReturn(address);
			if(i<=2){
				Mockito.when(host.getDatacenter()).thenReturn("dc1");				
			}else{
				Mockito.when(host.getDatacenter()).thenReturn("dc2");
			}
			hosts.add(host);			
		}		
		
		cluster = Mockito.mock(Cluster.class);
		policy.init(cluster, hosts);
    }

    @AfterMethod(groups = "short")
    public void tearDown() {
        policyLogger.setLevel(null);
        policyLogger.removeAppender(logs);
    }

		
	
	private Cluster.Builder builder() {
        return Cluster.builder()
            // Close cluster immediately to speed up tests.
            .withNettyOptions(nonQuietClusterCloseOptions);
    }
		

	@Test
	public void testDistance() throws UnknownHostException{
		int i=0;
		for(Host host:hosts){			
			policy.onUp(host);
			if(i<=2){
				// dc1 is local
				assertEquals(policy.distance(host),HostDistance.LOCAL);
			}else{
				assertTrue(policy.distance(host).equals(HostDistance.REMOTE) || policy.distance(host).equals(HostDistance.IGNORED));
			}
			i++;			
		}
	}
	
	@Test
	public void testLostOneNode() throws UnknownHostException{
		int i=0;		
		for(Host host:hosts){
			if(i==0){
				// we lost the first node
				// which doesn't trigger the switch
				policy.onDown(host);
			}
			
			if(i<=2){
				// dc1 is local
				assertEquals(policy.distance(host),HostDistance.LOCAL);
			}else{
				assertTrue(policy.distance(host).equals(HostDistance.REMOTE) || policy.distance(host).equals(HostDistance.IGNORED));
			}
			i++;			
		}
	}
	
	
	@Test
	public void testLostTwoNodes() throws UnknownHostException{
		int i=0;		
		
		for(Host host:hosts){
			if(i<=1){
				// we lost the first node
				// which doesn't trigger the switch
				policy.onDown(host);
			}
			i++;
		}
			
		i=0;
		for(Host host:hosts){	
			if(i<=2){
				// dc1 is remote now (lost 2 nodes)
				assertTrue(policy.distance(host).equals(HostDistance.REMOTE) || policy.distance(host).equals(HostDistance.IGNORED));				
			}else{
				assertEquals(policy.distance(host),HostDistance.LOCAL);
			}
			i++;			
		}
	}
	
	@Test
	public void testSwitchBackProtection() throws UnknownHostException{
		int i=0;		
		
		for(Host host:hosts){
			if(i<=1){
				// we lost the first node
				// which doesn't trigger the switch
				policy.onDown(host);
			}
			i++;
		}
				
		for(Host host:hosts){
			// first host is back up
			policy.onUp(host);
			break;
		}
		
			
		i=0;
		for(Host host:hosts){	
			if(i<=2){
				// dc1 is still remote now (lost 2 nodes, and got 1 back)
				assertTrue(policy.distance(host).equals(HostDistance.REMOTE) || policy.distance(host).equals(HostDistance.IGNORED));				
			}else{
				assertEquals(policy.distance(host),HostDistance.LOCAL);
			}
			i++;			
		}
	}
	
	/**
     * Ensures that {@link DCAwareFailoverRoundRobinPolicy} stays in local DC if we lost less nodes than the switch threshold.
     *
     * @test_category load_balancing:dc_aware
     */
    @Test(groups="short")
    public void shouldStayInLocalDcIfWeHaveEnoughNodes() {
        // given: a 10 node 2 DC cluster with DC policy with 2 remote hosts.
        ScassandraCluster sCluster = ScassandraCluster.builder().withNodes(5, 5).build();
        Cluster cluster = builder()
            .addContactPoint(sCluster.address(1, 1))
            .withLoadBalancingPolicy(new DCAwareFailoverRoundRobinPolicy(datacenter(1), datacenter(2), 3))
            .build();

        try {
            sCluster.init();

            Session session = cluster.connect();

            // when: a query is executed 50 times and some hosts are down in the local DC.
            sCluster.stop(cluster, 1, 5);
            sCluster.stop(cluster, 1, 3);            
            assertThat(cluster).controlHost().isNotNull();
            queryTracker.query(session, 60);

            // then: all requests should be distributed to the remaining up nodes in local DC.
            queryTracker.assertQueried(sCluster, 1, 1, 20);
            queryTracker.assertQueried(sCluster, 1, 2, 20);
            queryTracker.assertQueried(sCluster, 1, 4, 20);

            // then: no nodes in the remote DC should have been queried.
            for(int i = 1; i <= 5; i++) {
                queryTracker.assertQueried(sCluster, 2, i, 0);
            }
        } finally {
            cluster.close();
            sCluster.stop();
        }
    }
    
    
    /**
     * Ensures that {@link DCAwareFailoverRoundRobinPolicy} switches to backup DC if enough nodes are down in local DC.
     *
     * @test_category load_balancing:dc_aware
     */
    @Test(groups="short")
    public void shouldSwitchToBackupDcIfWeLostTooManyNodes() {
        // given: a 10 node 2 DC cluster with DC policy with 2 remote hosts.
        ScassandraCluster sCluster = ScassandraCluster.builder().withNodes(5, 5).build();
        Cluster cluster = builder()
            .addContactPoint(sCluster.address(1, 1))
            .withLoadBalancingPolicy(new DCAwareFailoverRoundRobinPolicy(datacenter(1), datacenter(2), 3))
            .build();

        try {
            sCluster.init();

            Session session = cluster.connect();

            // when: a query is executed 50 times and 3 hosts are down in the local DC.
            sCluster.stop(cluster, 1, 5);
            sCluster.stop(cluster, 1, 3);
            sCluster.stop(cluster, 1, 1);            
            assertThat(cluster).controlHost().isNotNull();
            queryTracker.query(session, 50);

            // then: all requests should be distributed to the nodes in backup DC.
            queryTracker.assertQueried(sCluster, 2, 1, 10);
            queryTracker.assertQueried(sCluster, 2, 2, 10);
            queryTracker.assertQueried(sCluster, 2, 3, 10);
            queryTracker.assertQueried(sCluster, 2, 4, 10);
            queryTracker.assertQueried(sCluster, 2, 5, 10);

            // then: no nodes in the local DC should have been queried.
            for(int i = 1; i <= 5; i++) {
                queryTracker.assertQueried(sCluster, 1, i, 0);
            }
            
            
        } finally {
            cluster.close();
            sCluster.stop();
        }
    }
    
    
    /**
     * Ensures that {@link DCAwareFailoverRoundRobinPolicy} switches to backup DC then sticks to it if nodes come 
     * back up in local DC.
     *
     * @test_category load_balancing:dc_aware
     */
    @Test(groups="short")
    public void shouldSwitchToBackupDcAndNeverSwitchBackToLocal() {
        // given: a 10 node 2 DC cluster with DC policy with 2 remote hosts.
        ScassandraCluster sCluster = ScassandraCluster.builder().withNodes(5, 5).build();
        Cluster cluster = builder()
            .addContactPoint(sCluster.address(1, 1))
            .withLoadBalancingPolicy(new DCAwareFailoverRoundRobinPolicy(datacenter(1), datacenter(2), 3))
            .build();

        try {
            sCluster.init();

            Session session = cluster.connect();

            // when: a query is executed 50 times and some hosts are down in the local DC.
            // and : those hosts come back up
            sCluster.stop(cluster, 1, 5);
            sCluster.stop(cluster, 1, 3);
            sCluster.stop(cluster, 1, 1);            
            assertThat(cluster).controlHost().isNotNull();
            queryTracker.query(session, 50);
            sCluster.start(cluster, 1, 5);
            sCluster.start(cluster, 1, 3);
            sCluster.start(cluster, 1, 1);

            // then: all requests should be distributed to the nodes in backup DC.
            queryTracker.assertQueried(sCluster, 2, 1, 10);
            queryTracker.assertQueried(sCluster, 2, 2, 10);
            queryTracker.assertQueried(sCluster, 2, 3, 10);
            queryTracker.assertQueried(sCluster, 2, 4, 10);
            queryTracker.assertQueried(sCluster, 2, 5, 10);

            // then: no nodes in the local DC should have been queried.
            for(int i = 1; i <= 5; i++) {
                queryTracker.assertQueried(sCluster, 1, i, 0);
            }
        } finally {
            cluster.close();
            sCluster.stop();
        }
    }
    
    /**
     * Ensures that {@link DCAwareFailoverRoundRobinPolicy} switches to backup DC then comes back to local DC if
     * conditions are fulfilled.
     * @throws InterruptedException 
     *
     * @test_category load_balancing:dc_aware
     */
    @Test(groups="short")
    public void shouldSwitchToBackupDcAndSwitchBackToLocalWhenPossible() throws InterruptedException {
        // given: a 10 node 2 DC cluster with DC policy with 2 remote hosts.
        ScassandraCluster sCluster = ScassandraCluster.builder().withNodes(5, 5).build();
        Cluster cluster = builder()
            .addContactPoint(sCluster.address(1, 1))
            .withLoadBalancingPolicy(
            		DCAwareFailoverRoundRobinPolicy.builder().withLocalDc(datacenter(1))
            																  .withBackupDc(datacenter(2))
            																  .withHostDownSwitchThreshold(3)
            																  .withSwitchBackDelayFactor((float) 2)
            																  .withNoSwitchBackDowntimeDelay(180).build())
            .build();

        try {
            sCluster.init();

            Session session = cluster.connect();

            // when: a query is executed 50 times and some hosts are down in the local DC.
            // and : those hosts come back up
            Date stoppedAt = new Date();
            sCluster.stop(cluster, 1, 5);
            sCluster.stop(cluster, 1, 3);
            sCluster.stop(cluster, 1, 1);            
            assertThat(cluster).controlHost().isNotNull();
            queryTracker.query(session, 50);            
            // wait 5 seconds
            Thread.sleep(5000);           
            sCluster.start(cluster, 1, 5);
            Date restartedAt = new Date();
            sCluster.start(cluster, 1, 3);
            sCluster.start(cluster, 1, 1);
            // then: all requests should be distributed to the nodes in the backup DC.
            queryTracker.query(session, 5);            
            queryTracker.assertQueried(sCluster, 2, 1, 11);
            queryTracker.assertQueried(sCluster, 2, 2, 11);
            queryTracker.assertQueried(sCluster, 2, 3, 11);
            queryTracker.assertQueried(sCluster, 2, 4, 11);
            queryTracker.assertQueried(sCluster, 2, 5, 11);
            // wait for another 30s to wait for a back switch to the local DC.
            Thread.sleep((restartedAt.getTime()-stoppedAt.getTime())*2);
            queryTracker.query(session, 5);
                     
            // then: 5 last queries should have been distributed to the local DC.
            queryTracker.assertQueried(sCluster, 1, 1, 1);
            queryTracker.assertQueried(sCluster, 1, 2, 1);
            queryTracker.assertQueried(sCluster, 1, 3, 1);
            queryTracker.assertQueried(sCluster, 1, 4, 1);
            queryTracker.assertQueried(sCluster, 1, 5, 1);
            

        } finally {
            cluster.close();
            sCluster.stop();
        }
    }
    


}
