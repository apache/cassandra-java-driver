/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.api.mapper.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A strategy to define which ancestors to scan for annotations for {@link Entity}-annotated classes
 * and {@link Dao}-annotated interfaces.
 *
 * <p>In addition, properties will also be scanned for on {@link Entity}-annotated classes.
 *
 * <p>By default, the mapper will transparently scan all parent classes and interfaces. The closer
 * in proximity a type is to the base type, the more precedence it is given when scanning. In
 * addition, parent classes are given precedence over interfaces.
 *
 * <p>For entities, this enables polymorphic mapping of a class hierarchy into different CQL tables
 * or UDTs.
 *
 * <p>For DAOs, this enables sharing configuration between DAOs by implementing an interface that
 * has annotations defining how the DAO methods should behave.
 *
 * <p>To disable scanning, set {@link #scanAncestors()} to <code>false</code>.
 *
 * <p>To control the highest ancestor considered in scanning for annotations, use {@link
 * #highestAncestor()} and {@link #includeHighestAncestor()}.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface HierarchyScanStrategy {

  /**
   * Whether or not ancestors should be scanned for properties and annotations.
   *
   * <p>If <code>false</code> only the {@link Entity} class or {@link Dao} interface will be
   * scanned.
   *
   * <p>Defaults to <code>true</code>.
   */
  boolean scanAncestors() default true;

  /**
   * The {@link Class} to consider the highest ancestor, meaning the classes that this class extends
   * or implements will not be scanned for annotations.
   *
   * <p>Note that If you have a complex hierarchy involving both parent classes and interfaces and
   * highestAncestor specifies a class for example, all interfaces will still be included. Therefore
   * it is recommended to avoid creating complex type hierarchies or to only do so if you expect the
   * entire hierarchy to be scanned.
   *
   * <p>Defaults to {@link Object}.
   */
  Class<?> highestAncestor() default Object.class;

  /**
   * Whether or not to include the specified {@link #highestAncestor()} in scanning.
   *
   * <p>Defaults to false.
   */
  boolean includeHighestAncestor() default false;
}
