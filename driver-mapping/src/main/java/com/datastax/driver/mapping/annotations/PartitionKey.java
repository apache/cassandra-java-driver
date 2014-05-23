package com.datastax.driver.mapping.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for fields that map to a CQL partition key (or one of it's
 * component if the partition key is composite).
 * <p>
 * If the partition key of the mapped table is composite, it is mandatory
 * to specify the ordinal parameter to avoid ordering ambiguity.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface PartitionKey {
    /**
     * Ordinal to add when the partition key has multiple components.
     *
     * @return the ordinal to use.
     */
    int value() default 0;
}
