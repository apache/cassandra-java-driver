/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.datastax.driver.core.orm;

/**
 * The replica placement strategy determines how replicas for a keyspace are
 * distributed across the cluster. The replica placement strategy is set when
 * you create a keyspace. You can choose from several strategies based on your
 * goals and the information you have about where nodes are located.
 * 
 * @author otavio
 */
public enum ReplicaStrategy {
    /**
     * SimpleStrategy places the first replica on a node determined by the
     * partitioner. Additional replicas are placed on the next nodes clockwise
     * in the ring without considering rack or data center location.
     */
    SIMPLES_TRATEGY("'SimpleStrategy'"),
    /**
     * is the preferred replication placement strategy when you have information
     * about how nodes are grouped in your data center, or when you have (or
     * plan to have) your cluster deployed across multiple data centers. This
     * strategy allows you to specify how many replicas you want in each data
     * center.
     */
    NETWORK_TOPOLOGY_STRATEGY("'NetworkTopologyStrategy'");

    private String value;

    ReplicaStrategy(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

}
