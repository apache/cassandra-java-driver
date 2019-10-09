/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.querybuilder;

import com.datastax.oss.driver.api.querybuilder.QueryBuilder;

/**
 * A DSE extension of the Cassandra driver's {@linkplain QueryBuilder query builder}.
 *
 * <p>Note that, at this time, this class acts a simple pass-through: there is no DSE-specific
 * syntax for DML queries, therefore it just inherits all of {@link QueryBuilder}'s methods, without
 * adding any of its own.
 *
 * <p>However, it is a good idea to use it as the entry point to the DSL in your DSE application, to
 * avoid changing all your imports if specialized methods get added here in the future.
 */
public class DseQueryBuilder extends QueryBuilder {
  // nothing to do
}
