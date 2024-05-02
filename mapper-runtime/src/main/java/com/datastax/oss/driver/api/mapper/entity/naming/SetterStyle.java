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

import com.datastax.oss.driver.api.mapper.annotations.PropertyStrategy;

/**
 * The style of setter that the mapper will look for when introspecting a mutable entity class.
 *
 * <p>Note that introspection always starts by looking for getters first (see {@link GetterStyle}).
 * Once a getter has been found, and if the entity is declared as {@link PropertyStrategy#mutable()
 * mutable}, the mapper will try to find a matching setter: name inferred as described below,
 * exactly one argument matching the property type, and the return type does not matter.
 *
 * @see PropertyStrategy
 */
public enum SetterStyle {

  /**
   * "JavaBeans" style: the method name must start with "set", for example {@code int foo =>
   * setFoo(int)}.
   */
  JAVABEANS,

  /**
   * "Fluent" style: the method name must be the name of the property, without any prefix, for
   * example {@code int foo => foo(int)}.
   */
  FLUENT,
  ;
}
