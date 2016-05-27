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

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Computed;
import com.datastax.driver.mapping.annotations.Table;

import java.lang.annotation.Annotation;
import java.util.*;

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

    @SuppressWarnings("unchecked")
    private static void validateAnnotations(Class<?> clazz, Class<? extends Annotation> allowed) {
        @SuppressWarnings("unchecked")
        Set<Annotation> classAnnotations = new HashSet<Annotation>();
        Collections.addAll(classAnnotations, clazz.getAnnotations());
        Class<? extends Annotation> invalid = validateAnnotations(classAnnotations, allowed);
        if (invalid != null)
            throw new IllegalArgumentException(String.format("Cannot have both @%s and @%s on type %s",
                    allowed.getSimpleName(), invalid.getSimpleName(),
                    clazz.getName()));
    }

    /**
     * Checks that a field is only annotated with the given mapping annotations, and that its "frozen" annotations are valid.
     */
    static void validateAnnotations(MappedProperty<?> property, Class<? extends Annotation>... allowed) {
        Class<? extends Annotation> invalid = validateAnnotations(property.annotations(), allowed);
        if (invalid != null)
            throw new IllegalArgumentException(String.format("Annotation @%s is not allowed on property %s",
                    invalid.getSimpleName(),
                    property));
        checkValidPrimaryKey(property);
        checkValidComputed(property);
    }

    // Returns the offending annotation if there is one
    private static Class<? extends Annotation> validateAnnotations(Collection<Annotation> annotations, Class<? extends Annotation>... allowed) {
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

    private static void checkValidPrimaryKey(MappedProperty<?> property) {
        if (property.isPartitionKey() && property.isClusteringColumn())
            throw new IllegalArgumentException("Property " + property.name() + " cannot have both the @PartitionKey and @ClusteringColumn annotations");
    }

    private static void checkValidComputed(MappedProperty<?> property) {
        if (property.isComputed()) {
            Computed computed = property.annotation(Computed.class);
            if (computed.value().isEmpty()) {
                throw new IllegalArgumentException(String.format("Property %s: attribute 'value' of annotation @Computed is mandatory for computed properties", property.name()));
            }
            if (property.hasAnnotation(Column.class)) {
                throw new IllegalArgumentException("Cannot use @Column and @Computed on the same property");
            }
        }
    }

    static <T> void validatePrimaryKeyOnUDT(MappedProperty<T> property) {
        switch (property.kind()) {
            case PARTITION_KEY:
                throw new IllegalArgumentException("Annotation @PartitionKey is not allowed in a class annotated by @UDT");
            case CLUSTERING_COLUMN:
                throw new IllegalArgumentException("Annotation @ClusteringColumn is not allowed in a class annotated by @UDT");
            default:
                break;
        }
    }

    static <T> void validateOrder(List<MappedProperty<T>> properties, String annotation) {
        for (int i = 0; i < properties.size(); i++) {
            MappedProperty<?> property = properties.get(i);
            int pos = property.position();
            if (pos != i)
                throw new IllegalArgumentException(String.format("Invalid ordering value %d for annotation %s of column %s, was expecting %d",
                        pos, annotation, property.name(), i));
        }
    }
}
