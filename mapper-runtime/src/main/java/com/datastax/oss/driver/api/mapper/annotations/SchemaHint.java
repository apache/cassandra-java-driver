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
package com.datastax.oss.driver.api.mapper.annotations;

import com.datastax.oss.driver.api.mapper.MapperBuilder;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates an entity to indicate which type of schema element it is supposed to map to. This is
 * only used to optimize {@linkplain MapperBuilder#withSchemaValidationEnabled(boolean) schema
 * validation}, it has no impact on query execution.
 *
 * <p>Example:
 *
 * <pre>
 * &#64;Entity
 * &#64;SchemaHint(targetElement = SchemaHint.TargetElement.TABLE)
 * public class Product {
 *   // fields of the entity
 * }
 * </pre>
 *
 * <p>By default, the mapper first tries to match the entity with a table, and if that doesn't work,
 * with a UDT. This annotation allows you to provide a hint as to which check should be done, so
 * that the mapper can skip the other one.
 *
 * <p>In addition, you can ask to completely skip the validation for this entity by using {@link
 * TargetElement#NONE}.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface SchemaHint {
  TargetElement targetElement();

  enum TargetElement {
    TABLE,
    UDT,
    NONE,
    ;
  }
}
