package com.datastax.driver.mapping.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An accessor is an interface that defines a set of method to read and write a
 * given table.
 * <p>
 * Each method declaration of an interface with the annotation {@link Accessor} must
 * have a {@link Query} annotation that defines the query the method executes.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Accessor {
    /** The name of the keyspace the table is part of. */
    String keyspace();
    /** The name of the table. */
    String name();

    /** Whether the keyspace name is a case sensitive one. */
    boolean caseSensitiveKeyspace() default false;
    /** Whether the table name is a case sensitive one. */
    boolean caseSensitiveTable() default false;
}
