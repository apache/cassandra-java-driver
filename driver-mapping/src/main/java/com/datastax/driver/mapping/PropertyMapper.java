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
 * A pluggable component that maps
 * Java properties to a Cassandra objects.
 */
public interface PropertyMapper {

    /**
     * Maps the given table class.
     *
     * @param tableClass the table class.
     * @return a set of mapped properties for the given class.
     */
    Set<? extends MappedProperty<?>> mapTable(Class<?> tableClass);

    /**
     * Maps the given UDT class.
     *
     * @param udtClass the UDT class.
     * @return a set of mapped properties for the given class.
     */
    Set<? extends MappedProperty<?>> mapUdt(Class<?> udtClass);

}
