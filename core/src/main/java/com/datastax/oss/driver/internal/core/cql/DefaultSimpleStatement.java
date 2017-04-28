/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import java.util.List;

public class DefaultSimpleStatement implements SimpleStatement {

  private final String query;
  private final List<Object> values;
  private final String configProfile;

  public DefaultSimpleStatement(String query, List<Object> values, String configProfile) {
    this.query = query;
    this.values = values;
    this.configProfile = configProfile;
  }

  @Override
  public String getQuery() {
    return query;
  }

  @Override
  public List<Object> getValues() {
    return values;
  }

  @Override
  public String getConfigProfile() {
    return configProfile;
  }
}
