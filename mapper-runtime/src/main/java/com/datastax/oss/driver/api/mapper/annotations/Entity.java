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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates a class that will be mapped to a Cassandra table or UDT.
 *
 * <p>Example:
 *
 * <pre>
 * &#64;Entity
 * public class Product {
 *   &#64;PartitionKey private UUID id;
 *   private String description;
 *
 *   public UUID getId() { return id; }
 *   public void setId(UUID id) { this.id = id; }
 *   public String getDescription() { return description; }
 *   public void setDescription(String description) { this.description = description; }
 * }
 * </pre>
 *
 * Entity classes follow the usual "POJO" conventions. Each property will be mapped to a CQL column.
 * In order to detect a property, the mapper processor looks for:
 *
 * <ul>
 *   <li>a getter method that follows the usual naming convention (e.g. {@code getDescription}) and
 *       has no parameters. The name of the property is obtained by removing the "get" prefix and
 *       decapitalizing ({@code description}), and the type of the property is the return type of
 *       the getter.
 *   <li>a matching setter method ({@code setDescription}), with a single parameter that has the
 *       same type as the property (the return type does not matter).
 *   <li>optionally, a matching field ({@code description}) that has the same type as the property.
 * </ul>
 *
 * Note that the field is not mandatory, a property can have only a getter and a setter (for example
 * if the value is computed, or the field has a different name, or is nested into another field,
 * etc.)
 *
 * <p>Properties can be annotated to configure various aspects of the mapping. The annotation can be
 * either on the field, or on the getter (if both are specified, the mapper processor issues a
 * compile-time warning, and the field annotation will be ignored). The available annotations are:
 *
 * <ul>
 *   <li>{@link PartitionKey}
 *   <li>{@link ClusteringColumn}
 *       <!-- TODO list new ones as they get added -->
 * </ul>
 *
 * <p>The class must expose a no-arg constructor that is at least package-private.
 *
 * <p>Entities are used as arguments or return types of {@link Dao} methods. They can also be nested
 * inside other entities (to map UDT columns).
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.CLASS)
public @interface Entity {}
