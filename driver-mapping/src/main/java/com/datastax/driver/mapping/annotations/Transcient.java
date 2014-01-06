package com.datastax.driver.mapping.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Whenever this annotation is added on a field, the field will not be mapped
 * to any column (neither during reads nor writes).
 * <p>
 * Please note that it is thus illegal to have a field that has both the
 * {@code Transcient} annotation and one of the {@link Column}, {@link PartitionKey}
 * or {@link ClusteringColumn} annotation.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Transcient {
}
