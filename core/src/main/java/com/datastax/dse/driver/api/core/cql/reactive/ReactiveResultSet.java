/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.cql.reactive;

import com.datastax.oss.driver.api.core.cql.Statement;
import org.reactivestreams.Publisher;

/**
 * A {@link Publisher} of {@link ReactiveRow}s returned by a {@link ReactiveSession}.
 *
 * <p>By default, all implementations returned by the driver are cold, unicast, single-subscriber
 * only publishers. In other words, <em>they do not support multiple subscriptions</em>; consider
 * caching the results produced by such publishers if you need to consume them by more than one
 * downstream subscriber.
 *
 * <p>Also, note that reactive result sets may emit items to their subscribers on an internal driver
 * IO thread. Subscriber implementors are encouraged to abide by <a
 * href="https://github.com/reactive-streams/reactive-streams-jvm#2.2">Reactive Streams
 * Specification rule 2.2</a> and avoid performing heavy computations or blocking calls inside
 * {@link org.reactivestreams.Subscriber#onNext(Object) onNext} calls, as doing so could slow down
 * the driver and impact performance. Instead, they should asynchronously dispatch received signals
 * to their processing logic.
 *
 * <p>This interface exists mainly to expose useful information about {@linkplain
 * #getExecutionInfos() request execution} and {@linkplain #getColumnDefinitions() query metadata}.
 * This is particularly convenient for queries that do not return rows; for queries that do return
 * rows, it is also possible, and oftentimes easier, to access that same information {@linkplain
 * ReactiveRow at row level}.
 *
 * @see ReactiveSession#executeReactive(String)
 * @see ReactiveSession#executeReactive(Statement)
 * @see ReactiveRow
 */
public interface ReactiveResultSet extends Publisher<ReactiveRow>, ReactiveQueryMetadata {}
