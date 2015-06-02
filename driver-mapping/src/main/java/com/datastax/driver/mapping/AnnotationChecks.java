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

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.lang.reflect.Field;
import java.util.*;

import com.datastax.driver.mapping.annotations.*;

/**
 * Various checks on mapping annotations.
 */
class AnnotationChecks {

    // The package containing the mapping annotations
    private static final Package MAPPING_PACKAGE = Table.class.getPackage();

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

        if (field.getAnnotation(Transient.class) == null) {
            try {
                checkFrozenTypes(field);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(String.format("Error while checking frozen types on field %s of %s %s: %s",
                                                                 field.getName(), classDescription,
                                                                 field.getDeclaringClass().getName(), e.getMessage()));
            }
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
        DeclaredFrozenType declaredFrozenType = getDeclaredFrozenType(field);
        checkFrozenTypes(javaType, declaredFrozenType);
    }

    // Builds a DeclaredFrozenType hierarchy based on the @Frozen* annotations on a field.
    private static DeclaredFrozenType getDeclaredFrozenType(Field field) {
        Frozen frozen = field.getAnnotation(Frozen.class);
        if (frozen != null)
            return DeclaredFrozenType.parse(frozen.value());

        boolean frozenKey = field.getAnnotation(FrozenKey.class) != null;
        boolean frozenValue = field.getAnnotation(FrozenValue.class) != null;
        if (frozenKey && frozenValue)
            return DeclaredFrozenType.FROZEN_MAP_KEY_AND_VALUE;
        else if (frozenKey)
            return DeclaredFrozenType.FROZEN_MAP_KEY;
        else if (frozenValue && field.getType().equals(Map.class))
            return DeclaredFrozenType.FROZEN_MAP_VALUE;
        else if (frozenValue)
            return DeclaredFrozenType.FROZEN_ELEMENT;
        else
            return DeclaredFrozenType.UNFROZEN_SIMPLE;
    }

    // Traverses the Java type and CQLType hierarchies in parallel, to ensure that all
    // Java types mapping to UDTs, tuples and nested collections are marked as frozen in the CQLType.
    // We accept that parts of the CQLType be null, in which case the matching parts
    // in the Java type will be considered as not frozen.
    private static void checkFrozenTypes(Type javaType, DeclaredFrozenType declaredFrozenType) {
        checkFrozenTypes(javaType, declaredFrozenType, true);
    }

    private static void checkFrozenTypes(Type javaType, DeclaredFrozenType declaredFrozenType, boolean isRoot) {
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

        boolean frozen = (declaredFrozenType != null && declaredFrozenType.frozen);
        checkValidFrozen(javaClass, isRoot, frozen);

        if (childrenJavaTypes != null) {
            for (int i = 0; i < childrenJavaTypes.length; i++) {
                Type childJavaType = childrenJavaTypes[i];
                DeclaredFrozenType childDeclaredFrozenType = null;
                if (declaredFrozenType != null && declaredFrozenType.subTypes != null && declaredFrozenType.subTypes.size() > i)
                    childDeclaredFrozenType = declaredFrozenType.subTypes.get(i);
                checkFrozenTypes(childJavaType, childDeclaredFrozenType, false);
            }
        }
    }

    private static void checkValidFrozen(Class<?> clazz, boolean isRoot, boolean isDeclaredFrozen) {
        boolean shouldBeFrozen = (TypeMappings.mapsToCollection(clazz) && !isRoot)
            || TypeMappings.mapsToUserTypeOrTuple(clazz);
        if (shouldBeFrozen != isDeclaredFrozen)
            throw new IllegalArgumentException(String.format("expected %s to be %sfrozen but was %sfrozen",
                clazz.getSimpleName(),
                shouldBeFrozen ? "" : "not ",
                isDeclaredFrozen ? "" : "not "));
    }
}
