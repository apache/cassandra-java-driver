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

import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Table;
import com.datastax.driver.mapping.annotations.UDT;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;

/**
 * Utility methods related to reflection.
 */
class ReflectionUtils {

    private static final Set<Class<? extends Annotation>> SUPPORTED_CLASS_ANNOTATIONS = ImmutableSet.of(
            Table.class,
            UDT.class,
            Accessor.class
    );

    static <T> T newInstance(Class<T> clazz) {
        Constructor<T> publicConstructor;
        try {
            publicConstructor = clazz.getConstructor();
        } catch (NoSuchMethodException e) {
            try {
                // try private constructor
                Constructor<T> privateConstructor = clazz.getDeclaredConstructor();
                privateConstructor.setAccessible(true);
                return privateConstructor.newInstance();
            } catch (Exception e1) {
                throw new IllegalArgumentException("Can't create an instance of " + clazz, e);
            }
        }
        try {
            return publicConstructor.newInstance();
        } catch (Exception e) {
            throw new IllegalArgumentException("Can't create an instance of " + clazz, e);
        }
    }

    // for each key representing a property name,
    // value[0] contains a Field object, value[1] contains a PropertyDescriptor object;
    // they cannot be both null at the same time
    static <T> Map<String, Object[]> scanFieldsAndProperties(Class<T> baseClass, MapperConfiguration.PropertyScanConfiguration scanConfiguration) {
        Map<String, Object[]> fieldsAndProperties = new HashMap<String, Object[]>();
        Map<String, Field> fields = scanFields(baseClass, scanConfiguration);
        for (Map.Entry<String, Field> entry : fields.entrySet()) {
            fieldsAndProperties.put(entry.getKey(), new Object[]{entry.getValue(), null});
        }
        Map<String, PropertyDescriptor> properties = scanProperties(baseClass, scanConfiguration);
        for (Map.Entry<String, PropertyDescriptor> entry : properties.entrySet()) {
            Object[] value = fieldsAndProperties.get(entry.getKey());
            if (value == null)
                fieldsAndProperties.put(entry.getKey(), new Object[]{null, entry.getValue()});
            else value[1] = entry.getValue();
        }
        return fieldsAndProperties;
    }

    private static <T> Map<String, Field> scanFields(Class<T> baseClass, MapperConfiguration.PropertyScanConfiguration scanConfiguration) {
        HashMap<String, Field> fields = new HashMap<String, Field>();
        // JAVA-1310: Make the annotation parsing logic configurable at mapper level
        // (only fields, only getters, or both)
        if (!scanConfiguration.getPropertyScanScope().isScanFields()) {
            return fields;
        }
        List<Class<?>> classesToScan = calculateClassesToScan(baseClass, scanConfiguration);
        for (Class<?> clazz : classesToScan) {
            for (Field field : clazz.getDeclaredFields()) {
                if (field.isSynthetic() || Modifier.isStatic(field.getModifiers()))
                    continue;
                // never override a more specific field masking another one declared in a superclass
                if (!fields.containsKey(field.getName()))
                    fields.put(field.getName(), field);
            }
        }
        return fields;
    }

    private static <T> Map<String, PropertyDescriptor> scanProperties(Class<T> baseClass, MapperConfiguration.PropertyScanConfiguration scanConfiguration) {
        Map<String, PropertyDescriptor> properties = new HashMap<String, PropertyDescriptor>();
        // JAVA-1310: Make the annotation parsing logic configurable at mapper level
        // (only fields, only getters, or both)
        if (!scanConfiguration.getPropertyScanScope().isScanGetters()) {
            return properties;
        }
        List<Class<?>> classesToScan = calculateClassesToScan(baseClass, scanConfiguration);
        for (Class<?> clazz : classesToScan) {
            // each time extract only current class properties
            BeanInfo beanInfo;
            try {
                beanInfo = Introspector.getBeanInfo(clazz, clazz.getSuperclass());
            } catch (IntrospectionException e) {
                throw Throwables.propagate(e);
            }
            for (PropertyDescriptor property : beanInfo.getPropertyDescriptors()) {
                if (!properties.containsKey(property.getName())) {
                    properties.put(property.getName(), property);
                }
            }
        }
        return properties;
    }

    private static List<Class<?>> calculateClassesToScan(Class<?> baseClass, MapperConfiguration.PropertyScanConfiguration scanConfiguration) {
        // JAVA-1310: Make the class hierarchy scan configurable at mapper level
        // (scan the whole hierarchy, or just annotated classes)
        List<Class<?>> classesToScan = new ArrayList<Class<?>>();
        MapperConfiguration.HierarchyScanStrategy scanStrategy = scanConfiguration.getHierarchyScanStrategy();
        Class<?> stopCondition = calculateStopConditionClass(baseClass, scanStrategy);
        for (Class<?> clazz = baseClass; clazz != null && !clazz.equals(stopCondition); clazz = clazz.getSuperclass()) {
            if (!scanStrategy.isScanOnlyAnnotatedClasses() || isClassAnnotated(clazz)) {
                classesToScan.add(clazz);
            }
        }
        return classesToScan;
    }

    private static Class<?> calculateStopConditionClass(Class<?> baseClass, MapperConfiguration.HierarchyScanStrategy scanStrategy) {
        // if scan is not enabled, stop at first parent
        if (!scanStrategy.isHierarchyScanEnabled()) {
            return baseClass.getSuperclass();
        }
        // if scan is enabled, stop at parent of deepest ancestor allowed
        Class<?> deepestAllowedAncestor = scanStrategy.getDeepestAllowedAncestor();
        return deepestAllowedAncestor == null ? null : deepestAllowedAncestor.getSuperclass();
    }

    private static boolean isClassAnnotated(Class<?> clazz) {
        for (Class<? extends Annotation> supportedClassAnnotation : SUPPORTED_CLASS_ANNOTATIONS) {
            if (clazz.getAnnotation(supportedClassAnnotation) != null) {
                return true;
            }
        }
        return false;
    }

    static Map<Class<? extends Annotation>, Annotation> scanPropertyAnnotations(Field field, PropertyDescriptor property) {
        Map<Class<? extends Annotation>, Annotation> annotations = new HashMap<Class<? extends Annotation>, Annotation>();
        // annotations on getters should have precedence over annotations on fields
        if (field != null)
            scanFieldAnnotations(field, annotations);
        Method getter = findGetter(property);
        if (getter != null)
            scanMethodAnnotations(getter, annotations);
        return annotations;
    }

    private static Map<Class<? extends Annotation>, Annotation> scanFieldAnnotations(Field field, Map<Class<? extends Annotation>, Annotation> annotations) {
        for (Annotation annotation : field.getAnnotations()) {
            annotations.put(annotation.annotationType(), annotation);
        }
        return annotations;
    }

    private static Map<Class<? extends Annotation>, Annotation> scanMethodAnnotations(Method method, Map<Class<? extends Annotation>, Annotation> annotations) {
        // 1. direct method annotations
        for (Annotation annotation : method.getAnnotations()) {
            annotations.put(annotation.annotationType(), annotation);
        }
        // 2. Class hierarchy: check for annotations in overridden methods in superclasses
        Class<?> getterClass = method.getDeclaringClass();
        for (Class<?> clazz = getterClass.getSuperclass(); clazz != null && !clazz.equals(Object.class); clazz = clazz.getSuperclass()) {
            maybeAddOverriddenMethodAnnotations(annotations, method, clazz);
        }
        // 3. Interfaces: check for annotations in implemented interfaces
        for (Class<?> clazz = getterClass; clazz != null && !clazz.equals(Object.class); clazz = clazz.getSuperclass()) {
            for (Class<?> itf : clazz.getInterfaces()) {
                maybeAddOverriddenMethodAnnotations(annotations, method, itf);
            }
        }
        return annotations;
    }

    private static void maybeAddOverriddenMethodAnnotations(Map<Class<? extends Annotation>, Annotation> annotations, Method getter, Class<?> clazz) {
        try {
            Method overriddenGetter = clazz.getDeclaredMethod(getter.getName(), (Class[]) getter.getParameterTypes());
            for (Annotation annotation : overriddenGetter.getAnnotations()) {
                // do not override a more specific version of the annotation type being scanned
                if (!annotations.containsKey(annotation.annotationType()))
                    annotations.put(annotation.annotationType(), annotation);
            }
        } catch (NoSuchMethodException e) {
            //ok
        }
    }

    static Method findGetter(PropertyDescriptor property) {
        if (property == null)
            return null;
        Method getter = property.getReadMethod();
        if (getter == null)
            return null;
        return getter;
    }

    static Method findSetter(Class<?> baseClass, PropertyDescriptor property) {
        if (property == null)
            return null;
        Method setter = property.getWriteMethod();
        if (setter != null)
            return setter;
        String propertyName = property.getName();
        String setterName = "set" + propertyName.substring(0, 1).toUpperCase() + propertyName.substring(1);
        // JAVA-984: look for a "relaxed" setter, ie. a setter whose return type may be anything
        try {
            setter = baseClass.getMethod(setterName, property.getPropertyType());
            if (!Modifier.isStatic(setter.getModifiers())) {
                return setter;
            }
        } catch (NoSuchMethodException e) {
            // ok
        }
        return null;
    }

    static void tryMakeAccessible(AccessibleObject object) {
        if (!object.isAccessible()) {
            try {
                object.setAccessible(true);
            } catch (SecurityException e) {
                // ok
            }
        }
    }

}
