package com.datastax.driver.mapping;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.*;

class DAOInvocationHandler<T> implements InvocationHandler {

    private final DAOMapper<T> mapper;

    private final Map<Method, MethodMapper> methodMap = new HashMap<Method, MethodMapper>();

    DAOInvocationHandler(DAOMapper<T> mapper) {
        this.mapper = mapper;

        for (MethodMapper method : mapper.methods)
            methodMap.put(method.method, method);
    }

    public Object invoke(Object proxy, Method m, Object[] args) throws Throwable {

        MethodMapper method = methodMap.get(m);
        if (mapper == null)
            throw new UnsupportedOperationException();

        return method.invoke(args);
    }
}

