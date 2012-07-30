package com.datastax.driver.core;

import java.util.Collection;
import java.util.Iterator;

/**
 * The policy that decides which Cassandra hosts to contact for each new query.
 *
 * The main method to implement is {@link LoadBalancingPolicy#newQueryPlan} and
 * is used for each query to find which host to query, and which hosts use as
 * failover.
 *
 * The {@code LoadBalancingPolicy} is a {@link Host.StateListener} and is thus
 * informed of hosts up/down events. For efficiency purposes, the policy is
 * expected to exclude down hosts from query plans.
 */
public interface LoadBalancingPolicy extends Host.StateListener {

    /**
     * Returns the hosts to use for a given query.
     *
     * Each new query will call this method. The first host in the result will
     * then be used to perform the query. In the even of a connection problem
     * (the queried host is down or appear to be so), the next host will be
     * used. If all hosts of the returned {@code Iterator} are down, the query
     * will fail.
     *
     * @return an iterator of Host. The query is tried against the hosts
     * returned by this iterator in order, until the query has been sent
     * successfully to one of the host.
     */
    public Iterator<Host> newQueryPlan();

    /**
     * Simple factory interface to allow creating {@link LoadBalancingPolicy} instances.
     */
    public interface Factory {

        /**
         * Creates a new LoadBalancingPolicy instance over the provided (initial) {@code hosts}.
         *
         * @param hosts the initial hosts to use.
         * @return the newly created {@link LoadBalancingPolicy} instance.
         */
        public LoadBalancingPolicy create(Collection<Host> hosts);
    }
}
