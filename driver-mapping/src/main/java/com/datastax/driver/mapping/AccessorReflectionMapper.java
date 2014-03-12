package com.datastax.driver.mapping;

import java.lang.reflect.Proxy;
import java.util.*;

class AccessorReflectionMapper<T> extends AccessorMapper<T> {

    private static AccessorReflectionFactory factory = new AccessorReflectionFactory();

    private final Class<T>[] proxyClasses;
    private final AccessorInvocationHandler<T> handler;

    @SuppressWarnings({"rawtypes", "unchecked"})
    private AccessorReflectionMapper(Class<T> daoClass, String keyspace, String table, List<MethodMapper> methods) {
        super(daoClass, keyspace, table, methods);
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
            throw new RuntimeException("Cannot create instance for Accessor interface " + daoClass.getName());
        }
    }

    private static class AccessorReflectionFactory implements Factory {
        public <T> AccessorMapper<T> create(Class<T> daoClass, String keyspace, String table, List<MethodMapper> methods) {
            return new AccessorReflectionMapper<T>(daoClass, keyspace, table, methods);
        }
    }
}
