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

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Field;

/**
 * Determines how Java property names are translated to Cassandra column/field names for a mapped
 * class.
 * <p/>
 * This will be used for any property that doesn't have an explicit name provided (via a
 * {@link Column} or {@link Field} annotation).
 * <p/>
 * If you need to implement your own strategy, the most straightforward approach is to build a
 * {@link DefaultNamingStrategy#DefaultNamingStrategy(NamingConvention, NamingConvention)
 * DefaultNamingStrategy with explicit naming conventions}.
 */
public interface NamingStrategy {

    /**
     * Infers a Cassandra column/field name from a Java property name.
     *
     * @param javaPropertyName the name of the Java property. Depending on the
     *                         {@link DefaultPropertyMapper#setPropertyAccessStrategy(PropertyAccessStrategy)
     *                         property access strategy}, this might the name of the Java field, or
     *                         be inferred from a getter/setter based on the usual Java beans
     *                         conventions.
     * @return the name of the Cassandra column or field. If you want the mapping to be
     * case-insensitive, this should be in lower case.
     */
    String toCassandraName(String javaPropertyName);

}
