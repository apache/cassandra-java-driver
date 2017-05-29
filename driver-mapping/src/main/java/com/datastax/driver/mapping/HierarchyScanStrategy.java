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

import java.util.List;

/**
 * A strategy to determine which ancestors of mapped classes should be scanned for mapped properties.
 */
public interface HierarchyScanStrategy {

    /**
     * Computes the ancestors of the given base class, optionally
     * filtering out any ancestor that should not be scanned.
     * <p/>
     * Implementors should always include {@code mappedClass}
     * in the returned list.
     *
     * @param mappedClass The mapped class; this is necessarily a class annotated with
     *                  either {@link com.datastax.driver.mapping.annotations.Table @Table} or
     *                  {@link com.datastax.driver.mapping.annotations.UDT @UDT}.
     * @return the list of classes that should be scanned,
     * including {@code mappedClass} itself and its ancestors,
     * ordered from the lowest (closest to {@code mappedClass})
     * to the highest (or farthest from {@code mappedClass}).
     */
    List<Class<?>> filterClassHierarchy(Class<?> mappedClass);

}
