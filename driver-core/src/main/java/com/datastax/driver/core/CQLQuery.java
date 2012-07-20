package com.datastax.driver.core;

/**
 * A marker interface for classes representing a CQL query.
 *
 * This interface require no specific method, but the toString() method of a
 * class implementing CQLQuery must return a CQL query string.
 */
public interface CQLQuery {}
