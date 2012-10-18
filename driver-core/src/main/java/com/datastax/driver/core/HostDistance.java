package com.datastax.driver.core;

/**
 * The distance to a Cassandra node as assigned by a
 * {@link LoadBalancingPolicy} (through its {@code distance} method).
 *
 * The distance assigned to an host influence how many connections the driver
 * maintains towards this host. If for a given host the assigned {@code HostDistance}
 * is {@code LOCAL} or {@code REMOTE}, some connections will be maintained by
 * the driver to this host. More active connections will be kept to
 * {@code LOCAL} host than to a {@code REMOTE} one (and thus well behaving
 * {@code LoadBalancingPolicy} should assign a {@code REMOTE} distance only to
 * hosts that are the less often queried).
 * <p>
 * However, if an host is assigned the distance {@code IGNORED}, no connection
 * to that host will maintained active. In other words, {@code IGNORED} should
 * be assigned to hosts that should not be used by this driver (because they
 * are in a remote datacenter for instance).
 */
public enum HostDistance {
    LOCAL,
    REMOTE,
    IGNORED
}
