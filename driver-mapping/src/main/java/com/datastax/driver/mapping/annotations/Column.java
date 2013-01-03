package com.datastax.driver.mapping.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation can optionally be added to each fields in entities whenever
 * the name of the column being mapped in Cassandra doesn't match the one of
 * the field in the Java entity.
 * If no String parameter is given, this annotation will simply indicate that
 * the field being considered is not transcient. Consequently, adding both this
 * annotation and a {@link Transcient} annotation to a field will lead to an
 * exception at startup.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {
	/**
	 * Name of the column being mapped in Cassandra.
	 */
	String name() default "";

}
