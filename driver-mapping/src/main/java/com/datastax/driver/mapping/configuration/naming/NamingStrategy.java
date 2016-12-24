/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.mapping.configuration.naming;

public class NamingStrategy {

    private boolean enabled;

    private NamingConvention javaConvention;

    private NamingConvention cassandraConvention;

    public NamingStrategy(NamingConvention javaConvention, NamingConvention cassandraConvention) {
        setJavaConvention(javaConvention);
        setCassandraConvention(cassandraConvention);
    }

    public NamingStrategy(NamingStrategy toCopy) {
        this.enabled = toCopy.enabled;
        this.javaConvention = toCopy.javaConvention;
        this.cassandraConvention = toCopy.cassandraConvention;
    }

    /**
     * Receive a String input assumed to be in the Java naming convention configured,
     * returns the String translation to the Cassandra naming convention.
     *
     * @param input value to translate
     * @return Cassandra naming convention translation of the input
     */
    public String toCassandra(String input) {
        if (!enabled || javaConvention.equals(cassandraConvention)) {
            return input;
        }
        return cassandraConvention.join(javaConvention.split(input));
    }

    /**
     * Receive a String input assumed to be in the Cassandra naming convention configured,
     * returns the String translation to the Java naming convention.
     *
     * @param input value to translate
     * @return Java naming convention translation of the input
     */
    public String toJava(String input) {
        if (!enabled || javaConvention.equals(cassandraConvention)) {
            return input;
        }
        return javaConvention.join(cassandraConvention.split(input));
    }

    /**
     * Enables the naming strategy, allowing it to auto translate properties that haven't been
     * explicitly translated.
     */
    public void enable() {
        enabled = true;
    }

    /**
     * Disables the naming strategy, causing the mapper to assume cassandra column names equals
     * to equivalent java properties unless explicitly defined
     */
    public void disable() {
        enabled = false;
    }

    /**
     * Returns the naming strategy Java convention
     *
     * @return the naming strategy Java convention
     */
    public NamingConvention getJavaConvention() {
        return javaConvention;
    }

    /**
     * Sets the naming strategy Java convention
     *
     * @param javaConvention the Java convention to use
     * @return the NamingStrategy to enable builder pattern
     */
    public NamingStrategy setJavaConvention(NamingConvention javaConvention) {
        if (javaConvention == null) {
            throw new IllegalArgumentException("input/output NamingConvention cannot be null");
        }
        this.javaConvention = javaConvention;
        return this;
    }


    /**
     * Returns the naming strategy Cassandra convention
     *
     * @return the naming strategy Cassandra convention
     */
    public NamingConvention getCassandraConvention() {
        return cassandraConvention;
    }


    /**
     * Sets the naming strategy Cassandra convention
     *
     * @param cassandraConvention the Cassandra convention to use
     * @return the NamingStrategy to enable builder pattern
     */
    public NamingStrategy setCassandraConvention(NamingConvention cassandraConvention) {
        if (cassandraConvention == null) {
            throw new IllegalArgumentException("input/output NamingConvention cannot be null");
        }
        this.cassandraConvention = cassandraConvention;
        return this;
    }

}
