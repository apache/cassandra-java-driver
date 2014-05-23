package com.datastax.driver.mapping.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.datastax.driver.core.Configuration;

/**
 * Query parameters to use in the (generated) implementation of a method of an {@link Accessor}
 * interface.
 * <p>
 * All the parameters of this annotation are optional, and when not provided default to whatever
 * default the {@code Cluster} instance used underneath are (those set in
 * {@link Configuration#getQueryOptions}).
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface QueryParameters {
    /**
     * The consistency level to use for the operation.
     *
     * @return the consistency level to use for the operation.
     */
    String consistency() default "";

    /**
     * The fetch size to use for paging the result of this operation.
     *
     * @return the fetch size to use for the operation.
     */
    int fetchSize() default -1;

    /**
     * Whether tracing should be enabled for this operation.
     *
     * @return whether tracing should be enabled for this operation.
     */
    boolean tracing() default false;
}
