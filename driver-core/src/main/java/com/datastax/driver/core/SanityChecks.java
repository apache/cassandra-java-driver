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
package com.datastax.driver.core;

import com.google.common.reflect.TypeToken;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;

class SanityChecks {

    /**
     * Performs a series of runtime checks to ensure the environment does not have any
     * incompatible libraries or configurations.  Depending on the severity of an
     * incompatibility an {@link IllegalStateException} may be thrown or an ERROR or
     * WARNING is logged.
     *
     * @throws IllegalStateException If an environment incompatibility is detected.
     * @see #checkGuava
     */
    static void check() {
        checkGuava();
    }

    /**
     * Detects if a version of guava older than 16.01 is present by attempting to create
     * a {@link TypeToken} instance for <code>Map&lt;String,String&gt;</code> and ensures that the
     * value type argument is of instance {@link String}.  If using an older version of guava
     * this will resolve to {@link Object} instead.  In this case an {@link IllegalStateException}
     * is thrown.
     *
     * @throws IllegalStateException if version of guava less than 16.01 is detected.
     */
    static void checkGuava() {
        boolean resolved = false;
        TypeToken<Map<String, String>> mapOfString = TypeTokens.mapOf(String.class, String.class);
        Type type = mapOfString.getType();
        if (type instanceof ParameterizedType) {
            ParameterizedType pType = (ParameterizedType) type;
            Type[] types = pType.getActualTypeArguments();
            if (types.length == 2) {
                TypeToken valueType = TypeToken.of(types[1]);
                resolved = valueType.getRawType().equals(String.class);
            }
        }

        if (!resolved) {
            throw new IllegalStateException(
                    "Detected Guava issue #1635 which indicates that a version of Guava less than 16.01 is in use.  "
                            + "This introduces codec resolution issues and potentially other incompatibility issues in the driver.  "
                            + "Please upgrade to Guava 16.01 or later.");
        }
    }

}
