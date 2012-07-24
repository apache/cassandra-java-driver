package com.datastax.driver.core;

import java.util.Collection;
import java.util.Iterator;

public interface LoadBalancingPolicy extends Host.StateListener {

    public void initialize(Collection<Host> hosts);

    public Iterator<Host> newQueryPlan();
}
