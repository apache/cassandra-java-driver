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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

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
            throw new IllegalArgumentException(String.format("@%s annotation was not found on %s",
                    annotation.getSimpleName(), annotatedClass));

        // Check that no other mapping annotations are present
        validateAnnotations(annotatedClass, annotation);

        return instance;
    }

    @SuppressWarnings("unchecked")
    private static void validateAnnotations(Class<?> clazz, Class<? extends Annotation> allowed) {
        @SuppressWarnings("unchecked")
        Collection<Annotation> classAnnotations = new HashSet<Annotation>();
        Collections.addAll(classAnnotations, clazz.getAnnotations());
        Class<? extends Annotation> invalid = validateAnnotations(classAnnotations, Collections.singleton(allowed));
        if (invalid != null)
            throw new IllegalArgumentException(String.format("Cannot have both @%s and @%s on %s",
                    allowed.getSimpleName(), invalid.getSimpleName(),
                    clazz));
    }

    /**
     * Checks that a field is only annotated with the given mapping annotations, and that its "frozen" annotations are valid.
     */
    static void validateAnnotations(PropertyMapper property, Collection<? extends Class<? extends Annotation>> allowed) {
        Class<? extends Annotation> invalid = validateAnnotations(property.getAnnotations(), allowed);
        if (invalid != null) {
            throw new IllegalArgumentException(String.format("Annotation @%s is not allowed on property '%s'",
                    invalid.getSimpleName(),
                    property));
        }
        checkValidPrimaryKey(property);
        checkValidComputed(property);
    }

    // Returns the offending annotation if there is one
    private static Class<? extends Annotation> validateAnnotations(Collection<Annotation> annotations, Collection<? extends Class<? extends Annotation>> allowed) {
        for (Annotation annotation : annotations) {
            Class<? extends Annotation> actual = annotation.annotationType();
            if (actual.getPackage().equals(MAPPING_PACKAGE) && !allowed.contains(actual))
                return actual;
        }
        return null;
    }

    private static void checkValidPrimaryKey(PropertyMapper property) {
        if (property.isPartitionKey() && property.isClusteringColumn())
            throw new IllegalArgumentException(String.format("Property '%s' cannot be annotated with both @PartitionKey and @ClusteringColumn", property));
    }

    private static void checkValidComputed(PropertyMapper property) {
        if (property.isComputed()) {
            Computed computed = property.annotation(Computed.class);
            if (computed.value().isEmpty()) {
                throw new IllegalArgumentException(String.format("Property '%s': attribute 'value' of annotation @Computed is mandatory for computed properties", property));
            }
            if (property.hasAnnotation(Column.class)) {
                throw new IllegalArgumentException(String.format("Property '%s' cannot be annotated with both @Column and @Computed", property));
            }
        }
    }

    static void validateOrder(List<PropertyMapper> properties, String annotation) {
        for (int i = 0; i < properties.size(); i++) {
            PropertyMapper property = properties.get(i);
            int pos = property.position;
            if (pos != i)
                throw new IllegalArgumentException(String.format("Invalid ordering value %d for annotation %s of property '%s', was expecting %d",
                        pos, annotation, property, i));
        }
    }
}
