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
package com.datastax.driver.mapping;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * Utility methods related to reflection.
 */
class ReflectionUtils {

    /**
     * Gets a type argument from a parameterized type.
     *
     * @param pt   the parameterized type.
     * @param arg  the index of the argument to retrieve.
     * @param name the name of the element (field or method argument).
     * @return the type argument.
     */
    static Class<?> getParam(ParameterizedType pt, int arg, String name) {
        Type ft = pt.getActualTypeArguments()[arg];
        if (!(ft instanceof Class))
            throw new IllegalArgumentException(String.format("Cannot map parameter of class %s for %s", pt, name));
        return (Class<?>) ft;
    }
}
