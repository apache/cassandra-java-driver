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

import com.datastax.driver.core.PreparedStatement;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.List;

abstract class AccessorMapper<T> {

    public final Class<T> daoClass;
    protected final List<MethodMapper> methods;

    protected AccessorMapper(Class<T> daoClass, List<MethodMapper> methods) {
        this.daoClass = daoClass;
        this.methods = methods;
    }

    abstract T createProxy();

    public void prepare(MappingManager manager) {
        List<ListenableFuture<PreparedStatement>> statements = new ArrayList<ListenableFuture<PreparedStatement>>(methods.size());

        for (MethodMapper method : methods)
            statements.add(manager.getSession().prepareAsync(method.queryString));

        try {
            List<PreparedStatement> preparedStatements = Futures.allAsList(statements).get();
            for (int i = 0; i < methods.size(); i++)
                methods.get(i).prepare(manager, preparedStatements.get(i));
        } catch (Exception e) {
            throw new RuntimeException("Error preparing queries for accessor " + daoClass.getSimpleName(), e);
        }
    }

    interface Factory {
        public <T> AccessorMapper<T> create(Class<T> daoClass, List<MethodMapper> methods);
    }
}
