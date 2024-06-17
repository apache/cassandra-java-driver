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

import com.datastax.oss.driver.api.core.CqlSession;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates an interface that will serve as the entry point to mapper features.
 *
 * <p>Example:
 *
 * <pre>
 * &#64;Mapper
 * public interface InventoryMapper {
 *   &#64;DaoFactory
 *   ProductDao productDao();
 * }
 * </pre>
 *
 * The mapper annotation processor will generate an implementation, and a builder that allows you to
 * create an instance from a {@link CqlSession}:
 *
 * <pre>
 * InventoryMapper inventoryMapper = new InventoryMapperBuilder(session).build();
 * </pre>
 *
 * By default, the builder's name is the name of the interface with the suffix "Builder", and it
 * resides in the same package. You can also use a custom name with {@link #builderName()}.
 *
 * <p>The interface should define one or more {@link DaoFactory} methods.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Mapper {

  /**
   * The <em>fully-qualified</em> name of the builder class that will get generated in order to
   * create instances of the manager, for example "com.mycompany.MyCustomBuilder".
   *
   * <p>If this is left empty (the default), the builder's name is the name of the interface with
   * the suffix "Builder", and it * resides in the same package.
   */
  String builderName() default "";
}
