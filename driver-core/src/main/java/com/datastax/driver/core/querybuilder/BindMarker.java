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
package com.datastax.driver.core.querybuilder;

/**
 * A CQL3 bind marker.
 * <p/>
 * This can be either an anonymous bind marker or a named one (but note that
 * named ones are only supported starting in Cassandra 2.0.1).
 * <p/>
 * Please note that to create a new bind maker object you should use
 * {@link QueryBuilder#bindMarker()} (anonymous marker) or
 * {@link QueryBuilder#bindMarker(String)} (named marker).
 */
public class BindMarker {
    static final BindMarker ANONYMOUS = new BindMarker(null);

    private final String name;

    BindMarker(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        if (name == null)
            return "?";

        return Utils.appendName(name, new StringBuilder(name.length() + 1).append(':')).toString();
    }
}
