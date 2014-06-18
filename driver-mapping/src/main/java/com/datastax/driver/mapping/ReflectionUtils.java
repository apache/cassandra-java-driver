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
     * @param pt the parameterized type.
     * @param arg the index of the argument to retrieve.
     * @param name the name of the element (field or method argument).
     * @return the type argument.
     */
    static Class<?> getParam(ParameterizedType pt, int arg, String name) {
        Type ft = pt.getActualTypeArguments()[arg];
        if (!(ft instanceof Class))
            throw new IllegalArgumentException(String.format("Cannot map parameter of class %s for %s", pt, name));
        return (Class<?>)ft;
    }
}
