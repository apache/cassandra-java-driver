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
 * Annotates the field or getter of an {@link Entity} property, to indicate that it will not be
 * mapped to any column (neither during reads nor writes).
 *
 * <p>Example:
 *
 * <pre>
 * &#64;Transient private int notAColumn;
 * </pre>
 *
 * This information is used by the mapper processor to exclude the property from generated queries.
 *
 * <p>Please note that {@code Transient} takes precedence over {@link PartitionKey} and {@link
 * ClusteringColumn} annotations.
 */
@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Transient {}
