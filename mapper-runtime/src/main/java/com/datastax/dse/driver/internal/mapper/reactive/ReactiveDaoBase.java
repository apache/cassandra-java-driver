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
package com.datastax.dse.driver.internal.mapper.reactive;

import com.datastax.dse.driver.api.core.cql.reactive.ReactiveResultSet;
import com.datastax.dse.driver.api.mapper.reactive.MappedReactiveResultSet;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.mapper.MapperContext;
import com.datastax.oss.driver.api.mapper.entity.EntityHelper;
import com.datastax.oss.driver.internal.mapper.DaoBase;

public class ReactiveDaoBase extends DaoBase {

  protected ReactiveDaoBase(MapperContext context) {
    super(context);
  }

  protected ReactiveResultSet executeReactive(Statement<?> statement) {
    return context.getSession().executeReactive(statement);
  }

  protected <EntityT> MappedReactiveResultSet<EntityT> executeReactiveAndMap(
      Statement<?> statement, EntityHelper<EntityT> entityHelper) {
    ReactiveResultSet source = executeReactive(statement);
    return new DefaultMappedReactiveResultSet<>(source, row -> entityHelper.get(row, false));
  }
}
