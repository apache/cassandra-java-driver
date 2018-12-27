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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a parameter of a DAO factory method in the main mapper class, to indicate that it must be
 * injected in the DAO as the table identifier.
 *
 * <p>Example:
 *
 * <pre>{@code @Mapper
 * public interface InventoryMapper {
 *   ProductDao productDao(@DaoTable String table);
 * }
 * }</pre>
 *
 * The annotated parameter can have type {@link String} or {@link CqlIdentifier}. If it is present,
 * the value will be injected in the DAO instance, where it will be used in generated queries, and
 * can be substituted in text queries. This allows you to reuse the same DAO for different tables:
 *
 * <pre>{@code
 * ProductDao productDao1 = inventoryMapper.productDao("product1");
 * ProductDao productDao2 = inventoryMapper.productDao("product2");
 * }</pre>
 *
 * The mapper will create an internal cache, and only create the DAO once for each keyspace and/or
 * table.
 *
 * @see DaoKeyspace
 */
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.CLASS)
public @interface DaoTable {}
