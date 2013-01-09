package com.datastax.driver.mapping.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation must be added on fields that map to partition key columns
 * in Cassandra.
 * When several partition keys are declared in a single entity, it is mandatory
 * to specify their ordinal parameter to avoid ordering ambiguity.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface PartitionKey {
    /**
     * Ordinal to add when several partition keys are declared within a single
     * entity.
     */
    int value() default 1;
}
