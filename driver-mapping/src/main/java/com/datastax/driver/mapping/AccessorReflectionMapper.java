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

import java.lang.reflect.Proxy;
import java.util.*;

class AccessorReflectionMapper<T> extends AccessorMapper<T> {

    private static AccessorReflectionFactory factory = new AccessorReflectionFactory();

    private final Class<T>[] proxyClasses;
    private final AccessorInvocationHandler<T> handler;

    @SuppressWarnings({"unchecked", "rawtypes"})
    private AccessorReflectionMapper(Class<T> daoClass, List<MethodMapper> methods) {
        super(daoClass, methods);
        this.proxyClasses = (Class<T>[])new Class[]{ daoClass };
        this.handler = new AccessorInvocationHandler<T>(this);
    }

    public static Factory factory() {
        return factory;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T createProxy() {
        try {
            return (T) Proxy.newProxyInstance(daoClass.getClassLoader(), proxyClasses, handler);
        } catch (Exception e) {
            throw new RuntimeException("Cannot create instance for Accessor interface " + daoClass.getName(), e);
        }
    }

    private static class AccessorReflectionFactory implements Factory {
        public <T> AccessorMapper<T> create(Class<T> daoClass, List<MethodMapper> methods) {
            return new AccessorReflectionMapper<T>(daoClass, methods);
        }
    }
}
