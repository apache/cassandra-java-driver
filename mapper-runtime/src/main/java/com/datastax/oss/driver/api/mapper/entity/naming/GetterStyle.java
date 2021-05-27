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
package com.datastax.oss.driver.api.mapper.entity.naming;

import com.datastax.oss.driver.api.mapper.annotations.PropertyStrategy;

/**
 * The style of getter that the mapper will look for when introspecting an entity class.
 *
 * <p>Note that introspection always starts by looking for getters first: no-arg, non-void methods
 * that follow the configured style. Then the mapper will try to find a matching field (which is not
 * required), and, if the entity is mutable, a setter.
 *
 * @see PropertyStrategy
 */
public enum GetterStyle {

  /**
   * "JavaBeans" style: the method name must start with "get", or "is" for boolean properties. The
   * name of the property is the getter name without a prefix, and decapitalized, for example {@code
   * int getFoo() => foo}.
   */
  JAVABEANS,

  /**
   * "Fluent" style: any name will match (as long as the no-arg, not-void rule also holds), and is
   * considered to be the property name without any prefix. For example {@code int foo() => foo}.
   *
   * <p>Note that this is the convention used in compiled Scala case classes. Whenever the mapper
   * processes a type that implements {@code scala.Product}, it will switch to this style by
   * default.
   */
  FLUENT,
  ;
}
