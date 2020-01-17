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
