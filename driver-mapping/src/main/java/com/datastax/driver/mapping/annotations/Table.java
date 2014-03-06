package com.datastax.driver.mapping.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Table {
    /** The name of the keyspace the table is part of. */
    String keyspace();
    /** The name of the table. */
    String name();

    /** Whether the keyspace name is a case sensitive one. */
    boolean caseSensitiveKeyspace() default false;
    /** Whether the table name is a case sensitive one. */
    boolean caseSensitiveTable() default false;

    String writeConsistency() default "";
    String readConsistency() default "";
}
