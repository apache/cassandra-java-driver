package com.datastax.driver.mapping.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.datastax.driver.mapping.EnumType;

/**
 * Defines that the annotated field (that must be a Java Enum) must be
 * persisted as an enumerated type.
 * <p>
 * The optional {@link EnumType} value defined how the enumeration must be
 * persisted.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Enumerated {
    /** How the enumeration must be persisted. */
    EnumType value() default EnumType.STRING;
}
