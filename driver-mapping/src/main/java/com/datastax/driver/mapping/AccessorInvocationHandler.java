/*
 *      Copyright (C) 2012-2014 DataStax Inc.
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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.*;

class AccessorInvocationHandler<T> implements InvocationHandler {

    private static final Object[] NO_ARGS = new Object[0];

    private final AccessorMapper<T> mapper;

    private final Map<Method, MethodMapper> methodMap = new HashMap<Method, MethodMapper>();

    AccessorInvocationHandler(AccessorMapper<T> mapper) {
        this.mapper = mapper;

        for (MethodMapper method : mapper.methods)
            methodMap.put(method.method, method);
    }

    public Object invoke(Object proxy, Method m, Object[] args) throws Throwable {

        MethodMapper method = methodMap.get(m);
        if (mapper == null)
            throw new UnsupportedOperationException();

        return method.invoke(args == null ? NO_ARGS : args);
    }
}

