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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates the parameter of a {@link DaoFactory} method that indicates the keyspace to create a
 * DAO for.
 *
 * <p>Example:
 *
 * <pre>
 * &#64;Mapper
 * public interface InventoryMapper {
 *   ProductDao productDao(@DaoKeyspace String ks);
 * }
 * </pre>
 *
 * The annotated parameter can be a {@link String} or {@link CqlIdentifier}. If it is present, the
 * value will be injected in the DAO instance, where it will be used in generated queries, and can
 * be substituted in text queries. This allows you to reuse the same DAO for different keyspaces.
 *
 * @see DaoFactory
 */
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
public @interface DaoKeyspace {}
