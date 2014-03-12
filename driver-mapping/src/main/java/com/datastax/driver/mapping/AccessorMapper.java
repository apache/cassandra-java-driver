package com.datastax.driver.mapping;

import java.lang.reflect.Method;
import java.util.*;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import com.datastax.driver.core.*;

abstract class AccessorMapper<T> {

    public final Class<T> daoClass;
    private final String keyspace;
    private final String table;
    protected final List<MethodMapper> methods;

    protected AccessorMapper(Class<T> daoClass, String keyspace, String table, List<MethodMapper> methods) {
        this.daoClass = daoClass;
        this.keyspace = keyspace;
        this.table = table;
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
            throw new RuntimeException("Error preparing queries for DAO " + daoClass.getName(), e);
        }
    }

    interface Factory {
        public <T> AccessorMapper<T> create(Class<T> daoClass, String keyspace, String table, List<MethodMapper> methods);
    }
}
