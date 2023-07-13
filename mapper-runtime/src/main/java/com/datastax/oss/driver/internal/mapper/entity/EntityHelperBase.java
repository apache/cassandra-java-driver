/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.mapper.entity;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.mapper.MapperContext;
import com.datastax.oss.driver.api.mapper.MapperException;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.entity.EntityHelper;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

public abstract class EntityHelperBase<EntityT> implements EntityHelper<EntityT> {

  protected final CqlIdentifier keyspaceId;

  protected final CqlIdentifier tableId;

  protected final MapperContext context;

  protected EntityHelperBase(MapperContext context, String defaultTableName) {
    this(context, null, defaultTableName);
  }

  protected EntityHelperBase(
      MapperContext context, String defaultKeyspaceName, String defaultTableName) {
    this.context = context;
    this.tableId =
        context.getTableId() != null
            ? context.getTableId()
            : CqlIdentifier.fromCql(defaultTableName);
    this.keyspaceId =
        context.getKeyspaceId() != null
            ? context.getKeyspaceId()
            : (defaultKeyspaceName == null ? null : CqlIdentifier.fromCql(defaultKeyspaceName));
  }

  @Nullable
  @Override
  public CqlIdentifier getKeyspaceId() {
    return keyspaceId;
  }

  @NonNull
  @Override
  public CqlIdentifier getTableId() {
    return tableId;
  }

  protected void throwIfKeyspaceMissing() {
    if (this.getKeyspaceId() == null && !context.getSession().getKeyspace().isPresent()) {
      throw new MapperException(
          String.format(
              "Missing keyspace. Suggestions: use SessionBuilder.withKeyspace() "
                  + "when creating your session, specify a default keyspace on %s with @%s"
                  + "(defaultKeyspace), or use a @%s method with a @%s parameter",
              this.getEntityClass().getSimpleName(),
              Entity.class.getSimpleName(),
              DaoFactory.class.getSimpleName(),
              DaoKeyspace.class.getSimpleName()));
    }
  }
}
