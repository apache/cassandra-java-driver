package com.datastax.driver.mapping.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation that allows to specify the name of the CQL column to which the
 * the field should be mapped.
 * <p>
 * Note that this annotation is generally optional in the sense that any field
 * of a class annotated by {@link Table} will be mapped by default to a column
 * having the same name than this field unless that field has the
 * {@link Transcient} annotation. As such, this annotation is mainly useful when
 * the name to map the field to is not the same one that the field itself (but
 * can be added without it's name parameter for documentation sake).
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {
    /**
     * Name of the column being mapped in Cassandra. By default, the name of the
     * field will be used.
     */
    String name() default "";
}
