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
