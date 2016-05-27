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

import com.google.common.base.Throwables;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility methods related to reflection.
 */
class ReflectionUtils {

    static <T> BeanInfo getBeanInfo(Class<T> udtClass) {
        BeanInfo beanInfo;
        try {
            beanInfo = Introspector.getBeanInfo(udtClass);
        } catch (IntrospectionException e) {
            throw Throwables.propagate(e);
        }
        return beanInfo;
    }

    static Map<Class<? extends Annotation>, Annotation> findAnnotations(PropertyDescriptor property, Class<?> baseClass) {
        // try field
        Map<Class<? extends Annotation>, Annotation> annotations = new HashMap<Class<? extends Annotation>, Annotation>();
        Field field = findField(property.getName(), baseClass);
        if (field != null) {
            for (Annotation annotation : field.getAnnotations()) {
                annotations.put(annotation.annotationType(), annotation);
            }
        }
        // try getter
        Method getter = findGetter(property);
        if (getter != null) {
            for (Annotation annotation : getter.getAnnotations()) {
                annotations.put(annotation.annotationType(), annotation);
            }
        }
        return annotations;
    }

    static Method findGetter(PropertyDescriptor property) {
        Method getter = property.getReadMethod();
        if (getter == null)
            return null;
        getter.setAccessible(true);
        return getter;
    }

    static Method findSetter(PropertyDescriptor property) {
        Method setter = property.getWriteMethod();
        if (setter == null)
            return null;
        setter.setAccessible(true);
        return setter;
    }

    static Field findField(String name, Class<?> baseClass) {
        for (Class<?> clazz = baseClass; !clazz.equals(Object.class); clazz = clazz.getSuperclass()) {
            try {
                Field field = clazz.getDeclaredField(name);
                if (field.isSynthetic() || (field.getModifiers() & Modifier.STATIC) == Modifier.STATIC)
                    continue;
                field.setAccessible(true);
                return field;
            } catch (NoSuchFieldException e) {
                //ok
            }
        }
        return null;
    }

}
