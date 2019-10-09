/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.cql.continuous.reactive;

import com.datastax.dse.driver.api.core.cql.reactive.ReactiveResultSet;
import com.datastax.oss.driver.api.core.cql.Statement;

/**
 * A marker interface for publishers returned by {@link ContinuousReactiveSession}.
 *
 * @see ContinuousReactiveSession#executeContinuouslyReactive(String)
 * @see ContinuousReactiveSession#executeContinuouslyReactive(Statement)
 */
public interface ContinuousReactiveResultSet extends ReactiveResultSet {}
