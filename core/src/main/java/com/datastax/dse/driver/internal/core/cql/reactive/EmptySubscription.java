/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.cql.reactive;

import org.reactivestreams.Subscription;

class EmptySubscription implements Subscription {

  static final EmptySubscription INSTANCE = new EmptySubscription();

  private EmptySubscription() {}

  @Override
  public void request(long n) {}

  @Override
  public void cancel() {}
}
