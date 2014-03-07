package com.datastax.driver.mapping;

import java.lang.reflect.Proxy;
import java.util.*;

class DAOReflectionMapper<T> extends DAOMapper<T> {

    private static DAOReflectionFactory factory = new DAOReflectionFactory();

    private final Class<T>[] proxyClasses;
    private final DAOInvocationHandler<T> handler;

    @SuppressWarnings({"rawtypes", "unchecked"})
    private DAOReflectionMapper(Class<T> daoClass, String keyspace, String table, List<MethodMapper> methods) {
        super(daoClass, keyspace, table, methods);
        this.proxyClasses = (Class<T>[])new Class[]{ daoClass };
        this.handler = new DAOInvocationHandler<T>(this);
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
            throw new RuntimeException("Cannot create instance for DAO interface " + daoClass.getName());
        }
    }

    private static class DAOReflectionFactory implements Factory {
        public <T> DAOMapper<T> create(Class<T> daoClass, String keyspace, String table, List<MethodMapper> methods) {
            return new DAOReflectionMapper<T>(daoClass, keyspace, table, methods);
        }
    }
}

