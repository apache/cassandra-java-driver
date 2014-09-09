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

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.lang.reflect.Field;
import java.util.*;

import com.google.common.primitives.Primitives;

import com.datastax.driver.mapping.annotations.*;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;

import com.datastax.driver.core.DataType;

/**
 * Various checks on mapping annotations.
 */
class AnnotationChecks {

    // The package containing the mapping annotations
    private static final Package MAPPING_PACKAGE = Table.class.getPackage();

    // The Java types that should not be annotated with @Frozen. Any other type is supposed
    // to map to a UDT or tuple.
    private static final Set<Class<?>> EXPECTED_NON_FROZEN_CLASSES;
    static {
        Builder<Class<?>> builder = ImmutableSet.<Class<?>>builder();
        for (DataType type : DataType.allPrimitiveTypes()) {
            builder.add(type.asJavaClass());
            builder.add(Primitives.unwrap(type.asJavaClass()));
        }
        builder.add(List.class);
        builder.add(Set.class);
        builder.add(Map.class);

        EXPECTED_NON_FROZEN_CLASSES = builder.build();
    }

    /**
     * Checks that a class is decorated with the given annotation, and return the annotation instance.
     * Also validates that no other mapping annotation is present.
     */
    static <T extends Annotation> T getTypeAnnotation(Class<T> annotation, Class<?> annotatedClass) {
        T instance = annotatedClass.getAnnotation(annotation);
        if (instance == null)
            throw new IllegalArgumentException(String.format("@%s annotation was not found on type %s",
                                                             annotation.getSimpleName(), annotatedClass.getName()));

        // Check that no other mapping annotations are present
        validateAnnotations(annotatedClass, annotation);

        return instance;
    }

    private static void validateAnnotations(Class<?> clazz, Class<? extends Annotation> allowed) {
        @SuppressWarnings("unchecked")
        Class<? extends Annotation> invalid = validateAnnotations(clazz.getAnnotations(), allowed);
        if (invalid != null)
            throw new IllegalArgumentException(String.format("Cannot have both @%s and @%s on type %s",
                                                             allowed.getSimpleName(), invalid.getSimpleName(),
                                                             clazz.getName()));
    }

    /**
     * Checks that a field is only annotated with the given mapping annotations, and that its "frozen" annotations are valid.
     */
    static void validateAnnotations(Field field, String classDescription, Class<? extends Annotation>... allowed) {
        Class<? extends Annotation> invalid = validateAnnotations(field.getAnnotations(), allowed);
        if (invalid != null)
            throw new IllegalArgumentException(String.format("Annotation @%s is not allowed on field %s of %s %s",
                                                             invalid.getSimpleName(),
                                                             field.getName(), classDescription,
                                                             field.getDeclaringClass().getName()));

        try {
            checkFrozenTypes(field);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format("Error while checking frozen types on field %s of %s %s: %s",
                                                             field.getName(), classDescription,
                                                             field.getDeclaringClass().getName(), e.getMessage()));
        }
    }

    // Returns the offending annotation if there is one
    private static Class<? extends Annotation> validateAnnotations(Annotation[] annotations, Class<? extends Annotation>... allowed) {
        for (Annotation annotation : annotations) {
            Class<? extends Annotation> actual = annotation.annotationType();
            if (actual.getPackage().equals(MAPPING_PACKAGE) && !contains(allowed, actual))
                return actual;
        }
        return null;
    }

    private static boolean contains(Object[] array, Object target) {
        for (Object element : array)
            if (element.equals(target))
                return true;
        return false;
    }

    static void checkFrozenTypes(Field field) {
        Type javaType = field.getGenericType();
        CQLType cqlType = getCQLType(field);
        checkFrozenTypes(javaType, cqlType);
    }

    // Builds a CQLType hierarchy based on the @Frozen* annotations on a field.
    private static CQLType getCQLType(Field field) {
        Frozen frozen = field.getAnnotation(Frozen.class);
        if (frozen != null)
            return CQLType.parse(frozen.value());

        boolean frozenKey = field.getAnnotation(FrozenKey.class) != null;
        boolean frozenValue = field.getAnnotation(FrozenValue.class) != null;
        if (frozenKey && frozenValue)
            return CQLType.FROZEN_MAP_KEY_AND_VALUE;
        else if (frozenKey)
            return CQLType.FROZEN_MAP_KEY;
        else if (frozenValue && field.getType().equals(Map.class))
            return CQLType.FROZEN_MAP_VALUE;
        else if (frozenValue)
            return CQLType.FROZEN_ELEMENT;
        else
            return CQLType.UNFROZEN_SIMPLE;
    }

    // Traverses the Java type and CQLType hierarchies in parallel, to ensure that all
    // Java types mapping to UDTs and tuples are marked as frozen in the CQLType.
    // We accept that parts of the CQLType be null, in which case the matching parts
    // in the Java type will be considered as not frozen.
    private static void checkFrozenTypes(Type javaType, CQLType cqlType) {
        Class<?> javaClass;
        Type[] childrenJavaTypes;
        if (javaType instanceof Class<?>) {
            javaClass = (Class<?>)javaType;
            childrenJavaTypes = null;
        } else if (javaType instanceof ParameterizedType){
            ParameterizedType pt = (ParameterizedType)javaType;
            javaClass = (Class<?>)pt.getRawType();
            childrenJavaTypes = pt.getActualTypeArguments();
        } else
            throw new IllegalArgumentException("unexpected type: " + javaType);

        boolean frozen = (cqlType != null && cqlType.frozen);
        checkValidFrozen(javaClass, frozen);

        if (childrenJavaTypes != null) {
            for (int i = 0; i < childrenJavaTypes.length; i++) {
                Type childJavaType = childrenJavaTypes[i];
                CQLType childCQLType = null;
                if (cqlType != null && cqlType.subTypes != null && cqlType.subTypes.size() > i)
                    childCQLType = cqlType.subTypes.get(i);
                checkFrozenTypes(childJavaType, childCQLType);
            }
        }
    }

    private static void checkValidFrozen(Class<?> clazz, boolean declared) {
        boolean expected = !EXPECTED_NON_FROZEN_CLASSES.contains(clazz) && !clazz.isEnum();
        if (expected != declared)
            throw new IllegalArgumentException(String.format("expected %s to be %sfrozen but was %sfrozen",
                                                             clazz.getSimpleName(),
                                                             expected ? "" : "not ",
                                                             declared ? "" : "not "));
    }
}
