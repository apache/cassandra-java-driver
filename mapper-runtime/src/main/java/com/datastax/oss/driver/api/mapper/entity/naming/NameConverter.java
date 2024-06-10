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
package com.datastax.oss.driver.api.mapper.entity.naming;

import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.NamingStrategy;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A custom converter to infer CQL column names from the names used in an {@link Entity}-annotated
 * class.
 *
 * @see NamingStrategy
 */
public interface NameConverter {

  /**
   * Convert the given Java name into a CQL name.
   *
   * <p>Note that this will be invoked by the generated code <em>each time</em> the name is
   * referenced. If the conversion is expensive, implementors might consider an internal cache.
   *
   * @param javaName the name to convert. Note that if it is capitalized (e.g. {@code Product}), it
   *     is a class name, to be converted into a table name; otherwise (e.g. {@code productId}), it
   *     is a property name, to be converted into a column name.
   * @return the corresponding CQL name. If you want it to be case-sensitive, it must be enclosed in
   *     double-quotes.
   */
  @NonNull
  String toCassandraName(@NonNull String javaName);
}
