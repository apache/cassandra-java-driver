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
package com.datastax.oss.driver.mapper.model;

import com.datastax.oss.driver.api.core.data.GettableByName;

/**
 * Provides a sample of an entity and means to check that it's correctly mapped to a driver data
 * structure.
 */
public abstract class EntityFixture<EntityT> {

  public final EntityT entity;

  protected EntityFixture(EntityT entity) {
    this.entity = entity;
  }

  public abstract void assertMatches(GettableByName data);
}
