package com.datastax.driver.mapping.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.datastax.driver.core.ConsistencyLevel;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Table {
    String name();
    String keyspace() default "";
    ConsistencyLevel defaultReadConsistencyLevel() default ConsistencyLevel.ONE;
    ConsistencyLevel defaultWriteConsistencyLevel() default ConsistencyLevel.ONE;
}
