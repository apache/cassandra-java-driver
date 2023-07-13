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
package com.datastax.oss.driver.api.mapper.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation could be used only on a class that is annotated with &#64;Entity annotation. The
 * logic will be applied only, if you are running mapper {@code withSchemaValidationEnabled(true)}.
 *
 * <p>Example:
 *
 * <pre>
 * &#64;Entity
 * &#64;SchemaHint(targetElement = &#64;SchemaHint.TargetElement.TABLE)
 * public class Product {
 *   // fields of the entity
 * }
 * </pre>
 *
 * <p>By default, if you will create an &#64;Entity without the &#64;SchemaHint annotation, the
 * following logic will be applied when doing validation:
 *
 * <ol>
 *   <li>Check if the given entity is a Table, if it is - validates if all fields of the Entity are
 *       present in the CQL table.
 *   <li>If it is not a table, check if the given entity is a UDT. If this is a case check if all
 *       Entity fields are present in the CQL UDT type.
 *   <li>If there is not information about Table or UDT it means that the given &#64;Entity has no
 *       corresponding CQL definition and error is generated.
 * </ol>
 *
 * <p>If you want the mapper to generate code only to check the path for UDT or Table you can
 * provide the &#64;SchemaHint on the Entity:
 *
 * <ol>
 *   <li>If you will set the {@code targetElement = TABLE}, then only the code path for checking CQL
 *       TABLE will be generated. If there is no corresponding CQL Table, then there is no check of
 *       UDT. The code throws an Exception denoting that CQL Table is missing for this Entity.
 *   <li>If you will set the {@code targetElement = UDT}, then only the code path for checking CQL
 *       UDT will be generated. If there is no corresponding CQL UDT type, the code throws an
 *       Exception denoting that CQL UDT is missing for this Entity.
 * </ol>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface SchemaHint {
  TargetElement targetElement();

  enum TargetElement {
    TABLE,
    UDT
  }
}
