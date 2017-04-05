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
 * A naming strategy that builds upon two naming conventions for the Java and Cassandra side.
 * <p/>
 * To infer a Cassandra name, the strategy will {@link NamingConvention#split(String) split}
 * the Java name according to the Java convention, and then
 * {@link NamingConvention#join(List) join} the resulting tokens according to the Cassandra
 * convention. For example, a possible implementation might:
 * <ul>
 *     <li>trim a prefix and tokenize on camel case: split("mUserName") = "user, name"</li>
 *     <li>join with a separator: join("user", "name") => "user_name"</li>
 * </ul>
 */
public class DefaultNamingStrategy implements NamingStrategy {

    private NamingConvention javaConvention;

    private NamingConvention cassandraConvention;

    /**
     * Builds a new instance with the default conventions, namely
     * {@link NamingConventions#LOWER_CAMEL_CASE lower camel case}
     * for the Java convention, and
     * {@link NamingConventions#LOWER_CASE lower case}
     * for the Cassandra convention.
     * <p/>
     * For example, a "userName" Java property will be mapped to a "username" column.
     */
    public DefaultNamingStrategy() {
        this(NamingConventions.LOWER_CAMEL_CASE, NamingConventions.LOWER_CASE);
    }

    /**
     * Builds a new instance.
     *
     * @param javaConvention      the naming convention that will be used to tokenize the Java
     *                            property names.
     * @param cassandraConvention the naming convention that will be used to build the Cassandra
     *                            names from the Java tokens.
     */
    public DefaultNamingStrategy(NamingConvention javaConvention, NamingConvention cassandraConvention) {
        if (javaConvention == null || cassandraConvention == null) {
            throw new IllegalArgumentException("input/output NamingConvention cannot be null");
        }
        this.javaConvention = javaConvention;
        this.cassandraConvention = cassandraConvention;
    }

    @Override
    public String toCassandraName(String javaPropertyName) {
        return cassandraConvention.join(javaConvention.split(javaPropertyName));
    }

}
