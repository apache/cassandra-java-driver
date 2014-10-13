package com.datastax.driver.core;

/**
 * Augment AssertJ with custom assertions for the Java driver.
 */
public class Assertions extends org.assertj.core.api.Assertions{
    public static ClusterAssert assertThat(Cluster cluster) {
        return new ClusterAssert(cluster);
    }
}
