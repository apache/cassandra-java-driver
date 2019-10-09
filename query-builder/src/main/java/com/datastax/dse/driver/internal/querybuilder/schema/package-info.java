/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */

/**
 * This package effectively mirrors the Cassandra OSS default query and schema implementations to
 * allow extended schema and query building for the DSE driver. In general, a class in this package
 * will need to implement the DSE equivalent interfaces for any DSE specific extensions.
 */
package com.datastax.dse.driver.internal.querybuilder.schema;
