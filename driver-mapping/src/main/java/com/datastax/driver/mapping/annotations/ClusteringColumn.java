package com.datastax.driver.mapping.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for fields that map to a CQL clustering column.
 * <p>
 * If the mapped table has multiple clustering columns, it is mandatory
 * to specify the ordinal parameter to avoid ordering ambiguity.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ClusteringColumn {
    /**
     * Ordinal to add when several clustering columns are declared within a single
     * entity.
     *
     * @return the ordinal value.
     */
    int value() default 0;
}
