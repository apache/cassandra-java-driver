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
package com.datastax.driver.core.policies;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.CloseableLoadBalancingPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy.Builder;

/**
 * A data-center aware Round-robin load balancing policy with DC failover
 * support.
 * <p>
 * This policy provides round-robin queries over the node of the local data
 * center. It also includes in the query plans returned a configurable number of
 * hosts in the remote data centers, but those are always tried after the local
 * nodes. In other words, this policy guarantees that no host in a remote data
 * center will be queried unless no host in the local data center can be
 * reached.
 * <p>
 * If used with a single data center, this policy is equivalent to the
 * {@code LoadBalancingPolicy.RoundRobin} policy, but its DC awareness incurs a
 * slight overhead so the {@code LoadBalancingPolicy.RoundRobin} policy could be
 * preferred to this policy in that case.
 * <p>
 * On top of the DCAwareRoundRobinPolicy, this policy uses a one way switch in
 * case a defined number of nodes are down in the local DC. As stated, the
 * policy never switches back to the local DC in order to prevent
 * inconsistencies and give ops teams the ability to repair the local DC before
 * switching back manually.
 */
public class DCAwareFailoverRoundRobinPolicy implements LoadBalancingPolicy,
		CloseableLoadBalancingPolicy {

	private static final Logger logger = LoggerFactory
			.getLogger(DCAwareFailoverRoundRobinPolicy.class);

	/**
     * Returns a builder to create a new instance.
     *
     * @return the builder.
     */
    public static Builder builder() {
        return new Builder();
    }
	
	private final String UNSET = "";

	private final ConcurrentMap<String, CopyOnWriteArrayList<Host>> perDcLiveHosts = new ConcurrentHashMap<String, CopyOnWriteArrayList<Host>>();
	private final AtomicInteger index = new AtomicInteger();

	@VisibleForTesting
	volatile String localDc;
	@VisibleForTesting
	volatile String backupDc;

	/**
	 * Current value of the switch threshold. if {@code hostDownSwitchThreshold}
	 * <= 0 then we must switch.
	 */
	private AtomicInteger hostDownSwitchThreshold = new AtomicInteger();
	
	/**
	 * Initial value of the switch threshold
	 */
	private final int initHostDownSwitchThreshold;

	/**
	 * flag to test if the switch as occurred
	 */
	private AtomicBoolean switchedToBackupDc = new AtomicBoolean(false);
	
	/**
	 * Time at which the switch occurred
	 */
	private Date switchedToBackupDcAt;

	/**
	 * Automatically switching back to local DC is possible after : downtime*
	 * {@code switchBackDelayFactor}
	 */
	private Float switchBackDelayFactor=(float)1000;

	/**
	 * Downtime delay after which switching back cannot be automated (usually
	 * when hinted handoff window is reached) In seconds.
	 */
	private int noSwitchBackDowntimeDelay=0;

	private Date localDcCameBackUpAt;
	private final int usedHostsPerRemoteDc;
	private final boolean dontHopForLocalCL;
	private boolean switchBackCanNeverHappen=false;

	private volatile Configuration configuration;

	/**
	 * Creates a new datacenter aware failover round robin policy that uses a
	 * local data-center and a backup data-center. Switching to the backup DC is
	 * triggered automatically if local DC loses more than
	 * {@code hostDownSwitchThreshold} nodes. Switching back to local DC after
	 * going to backup will never happen automatically.
	 * <p>
	 */

	public DCAwareFailoverRoundRobinPolicy(String localDc, String backupDc,
			int hostDownSwitchThreshold) {

		this(localDc, backupDc, hostDownSwitchThreshold, (float) -1.0, 0);

	}

	/**
	 * Creates a new datacenter aware failover round robin policy that uses a
	 * local data-center and a backup data-center. Switching to the backup DC is
	 * triggered automatically if local DC loses more than
	 * {@code hostDownSwitchThreshold} nodes.
	 * The policy will switch back to the local DC if conditions are fulfilled : 
	 * - Downtime lasted less than noSwitchBackDowntimeDelay (hint window)
	 * - uptime since downtime happened is > downtime*switchBackDelayFactor (give
	 * 	 enough time for hints to be executed)
	 * <p>
	 */

	public DCAwareFailoverRoundRobinPolicy(String localDc, String backupDc,
			int hostDownSwitchThreshold, float switchBackDelayFactor,
			int noSwitchBackDowntimeDelay) {
		this.localDc = localDc == null ? UNSET : localDc;
		this.backupDc = backupDc == null ? UNSET : backupDc;
		this.usedHostsPerRemoteDc = 0;
		this.dontHopForLocalCL = true;
		this.hostDownSwitchThreshold = new AtomicInteger(hostDownSwitchThreshold);
		this.initHostDownSwitchThreshold = hostDownSwitchThreshold;
		this.switchBackDelayFactor = switchBackDelayFactor;
		this.noSwitchBackDowntimeDelay = noSwitchBackDowntimeDelay;

	}

	public void init(Cluster cluster, Collection<Host> hosts) {
		if (localDc != UNSET)
			logger.info(
					"Using provided data-center name '{}' for DCAwareFailoverRoundRobinPolicy",
					localDc);

		this.configuration = cluster.getConfiguration();

		ArrayList<String> notInLocalDC = new ArrayList<String>();

		for (Host host : hosts) {
			String dc = dc(host);

			logger.trace("node {} is in dc {}", host.getAddress().toString(), dc);
			// If the localDC was in "auto-discover" mode and it's the first
			// host for which we have a DC, use it.
			if (localDc == UNSET && dc != UNSET) {
				logger.info(
						"Using data-center name '{}' for DCAwareFailoverRoundRobinPolicy (if this is incorrect, please provide the correct datacenter name with DCAwareFailoverRoundRobinPolicy constructor)",
						dc);
				localDc = dc;
			} else if (!dc.equals(localDc) && !dc.equals(backupDc))
				notInLocalDC.add(String.format("%s (%s)", host.toString(), dc));

			if (!dc.equals(localDc) && !dc.equals(backupDc))
				notInLocalDC.add(String.format("%s (%s)", host.toString(),
						host.getDatacenter()));

			CopyOnWriteArrayList<Host> prev = perDcLiveHosts.get(dc);
			if (prev == null)
				perDcLiveHosts.put(dc, new CopyOnWriteArrayList<Host>(
						Collections.singletonList(host)));
			else
				prev.addIfAbsent(host);
		}

		if (notInLocalDC.size() > 0) {
			String nonLocalHosts = Joiner.on(",").join(notInLocalDC);
			logger.warn(
					"Some contact points don't match local or backup data center. Local DC = {} - backup DC {}. Non-conforming contact points: {}",
					localDc, backupDc, nonLocalHosts);
		}
	}

	private String dc(Host host) {
		String dc = host.getDatacenter();
		return dc == null ? localDc : dc;
	}

	@SuppressWarnings("unchecked")
	private static CopyOnWriteArrayList<Host> cloneList(
			CopyOnWriteArrayList<Host> list) {
		return (CopyOnWriteArrayList<Host>) list.clone();
	}

	/**
	 * Return the HostDistance for the provided host.
	 * <p>
	 * This policy consider nodes in the local datacenter as {@code LOCAL}. For
	 * each remote datacenter, it considers a configurable number of hosts as
	 * {@code REMOTE} and the rest is {@code IGNORED}.
	 * <p>
	 * To configure how many host in each remote datacenter is considered
	 * {@code REMOTE}, see {@link #DCAwareRoundRobinPolicy(String, int)}.
	 *
	 * @param host
	 *            the host of which to return the distance of.
	 * @return the HostDistance to {@code host}.
	 */
	public HostDistance distance(Host host) {
		String dc = dc(host);
		// If the connection has switched to the backup DC and fulfills
		// the requirement for a back switch, make it happen.
		if(!switchBackCanNeverHappen){
			triggerBackSwitchIfNecessary();
		}

		if (isLocal(dc)) {			
			return HostDistance.LOCAL;
		}

		// Only hosts in local DC and backup DC can be considered remote
		if(dc(host).equals(localDc) || dc(host).equals(backupDc))
			return HostDistance.REMOTE;
		
		// All other hosts are ignored
		return HostDistance.IGNORED;
		
	}

	/**
	 * Returns the hosts to use for a new query.
	 * <p>
	 * The returned plan will always try each known host in the local datacenter
	 * first, and then, if none of the local host is reachable, will try up to a
	 * configurable number of other host per remote datacenter. The order of the
	 * local node in the returned query plan will follow a Round-robin
	 * algorithm.
	 *
	 * @param loggedKeyspace
	 *            the keyspace currently logged in on for this query.
	 * @param statement
	 *            the query for which to build the plan.
	 * @return a new query plan, i.e. an iterator indicating which host to try
	 *         first for querying, which one to use as failover, etc...
	 */
	public Iterator<Host> newQueryPlan(String loggedKeyspace,
			final Statement statement) {
		String currentDc = localDc;
		if(!switchBackCanNeverHappen){
			triggerBackSwitchIfNecessary();
		}

		if (switchedToBackupDc.get()) {
			currentDc = backupDc;
		}
		
		CopyOnWriteArrayList<Host> localLiveHosts = perDcLiveHosts
				.get(currentDc);
		final List<Host> hosts = localLiveHosts == null ? Collections
				.<Host> emptyList() : cloneList(localLiveHosts);
		final int startIdx = index.getAndIncrement();

		return new AbstractIterator<Host>() {

			private int idx = startIdx;
			private int remainingLocal = hosts.size();

			// For remote Dcs
			private Iterator<String> remoteDcs;
			private List<Host> currentDcHosts;
			private int currentDcRemaining;

			@Override
			protected Host computeNext() {				
				if (remainingLocal > 0) {
					remainingLocal--;					
					int c = idx++ % hosts.size();
					if (c < 0) {
						c += hosts.size();
					}					
					return hosts.get(c);
				}
									
				return endOfData();
			}
		};
	}

	public void onUp(Host host) {

		String dc = dc(host);		
		if (dc.equals(localDc) && this.hostDownSwitchThreshold.get() < this.initHostDownSwitchThreshold
				) {
			// if a node comes backup in the local DC and we're not already
			// equal to the initial threshold, add one node to the
			// switch threshold
			// This can only happen if the switch didn't occur yet
			this.hostDownSwitchThreshold.incrementAndGet();
			updateLocalDcStatus();
		}
		// If the localDC was in "auto-discover" mode and it's the first host
		// for which we have a DC, use it.
		if (localDc == UNSET && dc != UNSET) {
			logger.info(
					"Using data-center name '{}' for DCAwareFailoverRoundRobinPolicy (if this is incorrect, please provide the correct datacenter name with DCAwareFailoverRoundRobinPolicy constructor)",
					dc);
			localDc = dc;
		}

		CopyOnWriteArrayList<Host> dcHosts = perDcLiveHosts.get(dc);
		if (dcHosts == null) {
			CopyOnWriteArrayList<Host> newMap = new CopyOnWriteArrayList<Host>(
					Collections.singletonList(host));
			dcHosts = perDcLiveHosts.putIfAbsent(dc, newMap);
			// If we've successfully put our new host, we're good, otherwise
			// we've been beaten so continue
			if (dcHosts == null)
				return;
		}
		dcHosts.addIfAbsent(host);
	}

	public void onSuspected(Host host) {
	}

	public void onDown(Host host) {		
		if (dc(host).equals(localDc) && !switchedToBackupDc.get()) {
			// if a node goes down in the local DC remove one node to eventually
			// trigger the switch
			this.hostDownSwitchThreshold.decrementAndGet();
		}
		CopyOnWriteArrayList<Host> dcHosts = perDcLiveHosts.get(dc(host));
		if (dcHosts != null)
			dcHosts.remove(host);

		if (this.hostDownSwitchThreshold.get() <= 0) {
			// Make sure localDc is not considered as being up
			localDcCameBackUpAt = null;
			if (!switchedToBackupDc.get()) {
				// if we lost as many nodes in the local dc as configured in the
				// threshold, switch to backup DC
				switchToBackup();
			}
		}
	}

	public void onAdd(Host host) {
		onUp(host);
	}

	public void onRemove(Host host) {
		onDown(host);
	}

	public void close() {
		// nothing to do
	}

	/**
	 * Perform switch to backup DC
	 */
	private void switchToBackup() {
		switchedToBackupDc.set(true);
		switchedToBackupDcAt = new Date();
		logger.warn(
				"Lost {} nodes in data-center '{}'. Switching to data-center '{}'",
				this.initHostDownSwitchThreshold, this.localDc, this.backupDc);

	}

	/**
	 * Perform switch back to local DC
	 */
	private void switchBackToLocal() {
		switchedToBackupDc.set(false);
		switchedToBackupDcAt = null;
		localDcCameBackUpAt = null;
		logger.warn(
				"Recovered enough nodes in data-center '{}'. Switching back since conditions are fulfilled",
				this.localDc);

	}

	/**
	 * Check if the cluster state fulfills requirements for switching back to
	 * local DC. Conditions to switch back : - the connection as already
	 * switched to backup DC - hostDownSwitchThreshold is > 0 - Enough time has
	 * passed for hinted handoff (currentTime - localDcCameBackUpAt) >
	 * (localDcCameBackUpAt - switchedToBackupDcAt)*switchBackDelayFactor -
	 * (localDcCameBackUpAt - switchedToBackupDcAt) < noSwitchBackDowntimeDelay
	 * 
	 * @return
	 */
	private boolean canSwitchBack() {
		
		if ((localDcCameBackUpAt.getTime() - switchedToBackupDcAt.getTime()) < noSwitchBackDowntimeDelay * 1000) {
			if (switchedToBackupDc.get() && isLocalDcBackUp()) {
				logger.debug(
						"Local DC {} is up and has been down for {}s. Switch back will happen after {}s. Uptime = {}s ",
						localDc,
						(int) (getDowntimeDuration() / 1000),
						(int) (getDowntimeDuration() * switchBackDelayFactor / 1000),
						(getUptimeDuration()) / 1000);
				
				return (hostDownSwitchThreshold.get() > 0)
						&& (getUptimeDuration() > getDowntimeDuration() * switchBackDelayFactor)
						&& getDowntimeDuration() < noSwitchBackDowntimeDelay * 1000;
			}
		}else{
			// Downtime lasted more than the hinted handoff window
			// Switching back is now a manual operation
			logger.warn(
					"Local DC has been down for too long. Switch back will never happen.");
			switchBackCanNeverHappen=true;
		}

		return false;

	}
	
	/**
	 * returns the duration of the local DC downtime.
	 * @return
	 */
	private long getDowntimeDuration(){
		return localDcCameBackUpAt.getTime() - switchedToBackupDcAt.getTime();
	}
	
	/**
	 * get the uptime duration of local DC after outage.
	 * @return
	 */
	private long getUptimeDuration(){		
		return new Date().getTime() - localDcCameBackUpAt.getTime();
	}
	
	

	private void updateLocalDcStatus() {
		if (switchedToBackupDc.get() && hostDownSwitchThreshold.get() > 0 && localDcCameBackUpAt == null) {
			localDcCameBackUpAt = new Date();
		}
	}

	/**
	 * Test if local DC has enough nodes to be considered to be back up
	 * 
	 * @return
	 */
	private boolean isLocalDcBackUp() {
		return hostDownSwitchThreshold.get() > 0 && localDcCameBackUpAt != null;
	}

	/**
	 * Test if a node is in the local DC (or in the backup DC and switch has
	 * occurred)
	 * 
	 * @param dc
	 * @return
	 */
	private boolean isLocal(String dc) {
		return dc == UNSET || (dc.equals(localDc) && !switchedToBackupDc.get())
				|| (dc.equals(backupDc) && switchedToBackupDc.get());
	}

	/**
	 * Check if a switch as occurred and switching back to local DC is possible.
	 */
	public void triggerBackSwitchIfNecessary() {
		if (switchedToBackupDc.get() && localDcCameBackUpAt!=null && switchedToBackupDcAt!=null) {
			if (canSwitchBack()) {
				switchBackToLocal();
			}
		}
	}
	
	
	/**
     * Helper class to build the policy.
     */
    public static class Builder {
        private String localDc;
        private String backupDc;        
        private int hostDownSwitchThreshold;
        private Float switchBackDelayFactor=(float)1000;
    	private int noSwitchBackDowntimeDelay=0;

        /**
         * Sets the name of the datacenter that will be considered "local" by the policy.
         * <p>
         * This must be the name as known by Cassandra (in other words, the name in that appears in
         * {@code system.peers}, or in the output of admin tools like nodetool).
         * <p>
         * If this method isn't called, the policy will default to the datacenter of the first node
         * connected to. This will always be ok if all the contact points use at {@code Cluster}
         * creation are in the local data-center. Otherwise, you should provide the name yourself
         * with this method.
         *
         * @param localDc the name of the datacenter. It should not be {@code null}.
         * @return this builder.
         */
        public Builder withLocalDc(String localDc) {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(localDc),
                "localDc name can't be null or empty. If you want to let the policy autodetect the datacenter, don't call Builder.withLocalDC");
            this.localDc = localDc;
            return this;
        }
        
        /**
         * Sets the name of the datacenter that will be considered as "backup" by the policy.
         * <p>
         * This must be the name as known by Cassandra (in other words, the name in that appears in
         * {@code system.peers}, or in the output of admin tools like nodetool).
         * <p>
         * If this method must be called, otherwise you should not use this policy.
         *
         * @param backupDc the name of the datacenter. It should not be {@code null}.
         * @return this builder.
         */
        public Builder withBackupDc(String backupDc) {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(localDc),
                "backupDc name can't be null or empty.");
            this.backupDc = backupDc;
            return this;
        }

        
        /**
         * Sets how many nodes must be down in the local DC before switching to backup.  
         * 
         * @param hostDownSwitchThreshold the number of nodes down before switching to the backup DC.
         * @return this builder
         */
        public Builder withHostDownSwitchThreshold(int hostDownSwitchThreshold) {
            this.hostDownSwitchThreshold = hostDownSwitchThreshold;
            return this;
        }
        
        /**
         * Mandatory if you want to authorize switching back to local DC after downtime. 
         * Allows enough time to pass so that hinted handoff can finish 
         * (currentTime - localDcCameBackUpAt) > (localDcCameBackUpAt - switchedToBackupDcAt)*switchBackDelayFactor 
         * 
         * @param switchBackDelayFactor times downtime has to be <= uptime before switching back to local DC 
         * @return this builder
         */
        public Builder withSwitchBackDelayFactor(float switchBackDelayFactor) {
            this.switchBackDelayFactor = switchBackDelayFactor;
            return this;
        }
        
        /**
         * Mandatory if you want to authorize switching back to local DC after downtime.
         * Prevents switching back to local DC if downtime was longer than the provided value.
         * Used to check if downtime didn't last more than the hinted handoff window (which requires repair).
         * 
         * @param noSwitchBackDowntimeDelay max time in seconds before switching back to local DC will be prevented.
         * @return
         */
        public Builder withNoSwitchBackDowntimeDelay(int noSwitchBackDowntimeDelay) {
            this.noSwitchBackDowntimeDelay = noSwitchBackDowntimeDelay;
            return this;
        }
        
        

        /**
         * Builds the policy configured by this builder.
         *
         * @return the policy.
         */
        public DCAwareFailoverRoundRobinPolicy build() {
            return new DCAwareFailoverRoundRobinPolicy(localDc, backupDc, hostDownSwitchThreshold, switchBackDelayFactor, noSwitchBackDowntimeDelay);
        }
    }

}
