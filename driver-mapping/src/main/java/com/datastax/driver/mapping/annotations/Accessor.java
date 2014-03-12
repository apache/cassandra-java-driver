package com.datastax.driver.mapping.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An accessor is an interface that defines a set of method to read and write
 * from Cassandra.
 * <p>
 * Each method declaration of an interface with the annotation {@link Accessor} must
 * have a {@link Query} annotation that defines the query the method executes.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Accessor {}
