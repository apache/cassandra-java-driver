/*
 * Copyright (C) 2012-2017 DataStax Inc.
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
package com.datastax.driver.mapping;

import java.util.Set;

/**
 * A strategy to determine which properties are transient, and which aren't.
 * <p/>
 * Transient properties will be ignored, whereas non-transient
 * ones will be mapped.
 */
public enum PropertyTransienceStrategy {

    /**
     * This strategy adopts a permissive, opt-out approach that
     * will consider a property to be non-transient by default, unless:
     * <ol>
     * <li>The property is annotated with {@link com.datastax.driver.mapping.annotations.Transient @Transient};</li>
     * <li>The corresponding field is non-null and is marked with the keyword {@code transient};</li>
     * <li>The property name has been explicitly black-listed (see {@link DefaultPropertyMapper#setTransientPropertyNames(Set)}).</li>
     * </ol>
     */
    OPT_OUT,

    /**
     * This strategy adopts a conservative, opt-in approach that
     * only considers a property to be non-transient if it is explicitly annotated
     * with one of the following annotations:
     * <ol>
     * <li>{@link com.datastax.driver.mapping.annotations.Column Column}</li>
     * <li>{@link com.datastax.driver.mapping.annotations.Computed Computed}</li>
     * <li>{@link com.datastax.driver.mapping.annotations.ClusteringColumn ClusteringColumn}</li>
     * <li>{@link com.datastax.driver.mapping.annotations.Frozen Frozen}</li>
     * <li>{@link com.datastax.driver.mapping.annotations.FrozenKey FrozenKey}</li>
     * <li>{@link com.datastax.driver.mapping.annotations.FrozenValue FrozenValue}</li>
     * <li>{@link com.datastax.driver.mapping.annotations.PartitionKey PartitionKey}</li>
     * <li>{@link com.datastax.driver.mapping.annotations.Field Field}</li>
     * </ol>
     */
    OPT_IN

}
