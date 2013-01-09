package com.datastax.driver.mapping.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.datastax.driver.mapping.CounterMappingType;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface CounterMapping {
    CounterMappingType value();
}
