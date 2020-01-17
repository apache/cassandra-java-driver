/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.dse.driver.api.mapper.reactive;

import com.datastax.dse.driver.api.core.cql.reactive.ReactiveQueryMetadata;
import com.datastax.dse.driver.api.core.cql.reactive.ReactiveResultSet;
import org.reactivestreams.Publisher;

/**
 * A {@link Publisher} of mapped entities returned by DAO methods. In other words, this interface is
 * the equivalent of {@link ReactiveResultSet} for mapped entities.
 *
 * <p>By default, all implementations returned by the driver are cold, unicast, single-subscriber
 * only publishers. In other words, <em>they do not support multiple subscriptions</em>; consider
 * caching the results produced by such publishers if you need to consume them by more than one
 * downstream subscriber.
 *
 * <p>Also, note that mapped reactive result sets may emit items to their subscribers on an internal
 * driver IO thread. Subscriber implementors are encouraged to abide by <a
 * href="https://github.com/reactive-streams/reactive-streams-jvm#2.2">Reactive Streams
 * Specification rule 2.2</a> and avoid performing heavy computations or blocking calls inside
 * {@link org.reactivestreams.Subscriber#onNext(Object) onNext} calls, as doing so could slow down
 * the driver and impact performance. Instead, they should asynchronously dispatch received signals
 * to their processing logic.
 *
 * <p>This type is located in a {@code dse} package for historical reasons; reactive result sets
 * work with both Cassandra and DSE.
 *
 * @see ReactiveResultSet
 */
public interface MappedReactiveResultSet<EntityT>
    extends Publisher<EntityT>, ReactiveQueryMetadata {}
