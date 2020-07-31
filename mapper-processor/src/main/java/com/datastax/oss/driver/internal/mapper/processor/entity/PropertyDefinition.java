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
package com.datastax.oss.driver.internal.mapper.processor.entity;

import com.datastax.oss.driver.api.core.data.GettableByName;
import com.datastax.oss.driver.api.core.data.SettableByName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PropertyStrategy;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.PropertyType;
import com.squareup.javapoet.CodeBlock;

/**
 * Defines a property belonging to an {@link Entity}, how it should be used in forming a CQL query,
 * and how to extract and set the value of the property on the {@link Entity}.
 */
public interface PropertyDefinition {

  /**
   * @return the name of the property, in the JavaBeans sense. In other words this is {@link
   *     #getGetterName()} minus the "get" prefix and decapitalized.
   */
  String getJavaName();

  /**
   * @return A Java snippet that produces the corresponding expression in a <code>SELECT</code>
   *     statement, for example:
   *     <ul>
   *       <li><code>"id"</code> in <code>selectFrom.column("id")</code> for a regular column.
   *       <li><code>"writetime(v)"</code> in <code>selectFrom.raw("writetime(v)")</code> for a
   *           computed value.
   *     </ul>
   */
  CodeBlock getSelector();

  /**
   * @return A Java snippet that produces the name of the property in a {@link GettableByName} or
   *     {@link SettableByName}, for example <code>"id"</code> in <code>row.get("id")</code>.
   */
  CodeBlock getCqlName();

  /**
   * @return The name of the "get" method associated with this property used to retrieve the value
   *     of the property from the entity.
   */
  String getGetterName();

  /**
   * @return The name of the "set" method associated with this property used to update the value of
   *     the property on the entity, or {@code null} if the entity was marked as not {@link
   *     PropertyStrategy#mutable()}.
   */
  String getSetterName();

  /**
   * @return The {@link PropertyType} of this definition, which dictates how to interact with this
   *     property.
   */
  PropertyType getType();
}
