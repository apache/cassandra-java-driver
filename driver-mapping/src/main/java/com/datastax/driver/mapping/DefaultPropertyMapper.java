/*
 * Copyright (C) 2012-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.mapping;

import com.datastax.driver.core.Metadata;
import com.datastax.driver.mapping.annotations.*;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The default {@link PropertyMapper} used by the mapper.
 * <p/>
 * This mapper can be configured to scan for fields, getters and setters, or both.
 * The default is to scan for both.
 * <p/>
 * This mapper can also be configured to skip transient properties.
 * By default, all properties will be mapped (non-transient),
 * unless explicitly marked with {@link Transient @Transient}.
 * <p/>
 * This mapper recognizes standard getter and setter methods
 * (as defined by the Java Beans specification),
 * and also "relaxed" setter methods, i.e., setter methods
 * whose return type are not {@code void}.
 *
 * @see DefaultMappedProperty
 */
public class DefaultPropertyMapper implements PropertyMapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPropertyMapper.class);

    private static final HashSet<String> DEFAULT_TRANSIENT_PROPERTY_NAMES = Sets.newHashSet(
            "class",
            // JAVA-1279: exclude Groovy's metaClass property
            "metaClass"
    );

    private static final Set<Class<?>> NON_TRANSIENT_ANNOTATIONS = ImmutableSet.<Class<?>>of(
            Column.class,
            PartitionKey.class,
            ClusteringColumn.class,
            com.datastax.driver.mapping.annotations.Field.class,
            Computed.class,
            Frozen.class,
            FrozenKey.class,
            FrozenValue.class
    );

    /**
     * Annotations allowed on a property that maps to a table column.
     */
    private static final Set<Class<? extends Annotation>> VALID_COLUMN_ANNOTATIONS = ImmutableSet.<Class<? extends Annotation>>builder()
            .add(Column.class)
            .add(Computed.class)
            .add(ClusteringColumn.class)
            .add(Frozen.class)
            .add(FrozenKey.class)
            .add(FrozenValue.class)
            .add(PartitionKey.class)
            .add(Transient.class)
            .build();

    /**
     * Annotations allowed on a property that maps to a UDT field.
     */
    private static final Set<Class<? extends Annotation>> VALID_FIELD_ANNOTATIONS = ImmutableSet.of(
            com.datastax.driver.mapping.annotations.Field.class,
            Frozen.class,
            FrozenKey.class,
            FrozenValue.class,
            Transient.class);

    private PropertyAccessStrategy propertyAccessStrategy = PropertyAccessStrategy.BOTH;

    private PropertyTransienceStrategy propertyTransienceStrategy = PropertyTransienceStrategy.OPT_OUT;

    private HierarchyScanStrategy hierarchyScanStrategy = new DefaultHierarchyScanStrategy();

    private NamingStrategy namingStrategy = new DefaultNamingStrategy();

    private Set<String> transientPropertyNames = new HashSet<String>(DEFAULT_TRANSIENT_PROPERTY_NAMES);

    /**
     * Sets the {@link PropertyAccessStrategy property access strategy} to use.
     * The default is {@link PropertyAccessStrategy#BOTH}.
     *
     * @param propertyAccessStrategy the {@link PropertyAccessStrategy property access strategy} to use; may not be {@code null}.
     * @return this {@link DefaultPropertyMapper} instance (to allow for fluent builder pattern).
     */
    public DefaultPropertyMapper setPropertyAccessStrategy(PropertyAccessStrategy propertyAccessStrategy) {
        this.propertyAccessStrategy = checkNotNull(propertyAccessStrategy);
        return this;
    }

    /**
     * Sets the {@link PropertyTransienceStrategy property transience strategy} to use.
     * The default is {@link PropertyTransienceStrategy#OPT_OUT}.
     *
     * @param propertyTransienceStrategy the {@link PropertyTransienceStrategy property transience strategy} to use; may not be {@code null}.
     * @return this {@link DefaultPropertyMapper} instance (to allow for fluent builder pattern).
     */
    public DefaultPropertyMapper setPropertyTransienceStrategy(PropertyTransienceStrategy propertyTransienceStrategy) {
        this.propertyTransienceStrategy = checkNotNull(propertyTransienceStrategy);
        return this;
    }

    /**
     * Sets the {@link HierarchyScanStrategy hierarchy scan strategy} to use.
     * The default is {@link DefaultHierarchyScanStrategy}.
     *
     * @param hierarchyScanStrategy the {@link HierarchyScanStrategy hierarchy scan strategy} to use; may not be {@code null}.
     * @return this {@link DefaultPropertyMapper} instance (to allow for fluent builder pattern).
     */
    public DefaultPropertyMapper setHierarchyScanStrategy(HierarchyScanStrategy hierarchyScanStrategy) {
        this.hierarchyScanStrategy = checkNotNull(hierarchyScanStrategy);
        return this;
    }

    /**
     * Sets the {@link NamingStrategy naming strategy} to use.
     * The default is {@link DefaultNamingStrategy}.
     *
     * @param namingStrategy the {@link NamingStrategy naming strategy} to use; may not be {@code null}.
     * @return this {@link DefaultPropertyMapper} instance (to allow for fluent builder pattern).
     */
    public DefaultPropertyMapper setNamingStrategy(NamingStrategy namingStrategy) {
        this.namingStrategy = checkNotNull(namingStrategy);
        return this;
    }

    /**
     * Sets transient property names. This
     * will completely replace any names already configured for this object.
     * <p/>
     * The default set comprises the following property names:
     * {@code class} and {@code metaClass}.
     * These properties pertain to the {@link Object} class –
     * {@code metaClass} being specific to the Groovy language.
     * <p/>
     * Property names provided here will always be considered transient;
     * if a more fine-grained tuning is required, it is also possible
     * to use the {@link Transient @Transient} annotation
     * on a specific property.
     * <p/>
     * Subclasses can also override {@link #isTransient(String, Field, Method, Method, Map)} to gain
     * complete control over which properties should be considered transient.
     *
     * @param transientPropertyNames a set of property names to exclude from mapping; may not be {@code null}. This
     *                               will completely replace any names already configured for this object.
     */
    public DefaultPropertyMapper setTransientPropertyNames(Set<String> transientPropertyNames) {
        this.transientPropertyNames = checkNotNull(transientPropertyNames);
        return this;
    }

    /**
     * Adds new values to the existing set of transient property names.
     * <p/>
     * The default set comprises the following property names:
     * {@code class} and {@code metaClass}.
     * These properties pertain to the {@link Object} class –
     * {@code metaClass} being specific to the Groovy language.
     * <p/>
     * Property names provided here will always be considered transient;
     * if a more fine-grained tuning is required, it is also possible
     * to use the {@link Transient @Transient} annotation
     * on a specific property.
     * <p/>
     * Subclasses can also override {@link #isTransient(String, Field, Method, Method, Map)} to gain
     * complete control over which properties should be considered transient.
     *
     * @param transientPropertyNames the values to add; may not be {@code null}.
     */
    public DefaultPropertyMapper addTransientPropertyNames(String... transientPropertyNames) {
        return addTransientPropertyNames(Arrays.asList(checkNotNull(transientPropertyNames)));
    }

    /**
     * Adds new values to the existing set of transient property names.
     * <p/>
     * The default set comprises the following property names:
     * {@code class} and {@code metaClass}.
     * These properties pertain to the {@link Object} class –
     * {@code metaClass} being specific to the Groovy language.
     * <p/>
     * Property names provided here will always be considered transient;
     * if a more fine-grained tuning is required, it is also possible
     * to use the {@link Transient @Transient} annotation
     * on a specific property.
     * <p/>
     * Subclasses can also override {@link #isTransient(String, Field, Method, Method, Map)} to gain
     * complete control over which properties should be considered transient.
     *
     * @param transientPropertyNames the values to add; may not be {@code null}.
     */
    public DefaultPropertyMapper addTransientPropertyNames(Collection<String> transientPropertyNames) {
        this.transientPropertyNames.addAll(checkNotNull(transientPropertyNames));
        return this;
    }

    @Override
    public Set<? extends MappedProperty<?>> mapTable(Class<?> tableClass) {
        return mapTableOrUdt(tableClass, VALID_COLUMN_ANNOTATIONS);
    }

    @Override
    public Set<? extends MappedProperty<?>> mapUdt(Class<?> udtClass) {
        return mapTableOrUdt(udtClass, VALID_FIELD_ANNOTATIONS);
    }

    private Set<? extends MappedProperty<?>> mapTableOrUdt(Class<?> entityClass, Collection<? extends Class<? extends Annotation>> allowed) {
        Map<String, Object[]> fieldsGettersAndSetters = new HashMap<String, Object[]>();
        List<Class<?>> classHierarchy = hierarchyScanStrategy.filterClassHierarchy(entityClass);
        if (propertyAccessStrategy.isFieldScanAllowed()) {
            Map<String, Field> fields = scanFields(classHierarchy);
            for (Map.Entry<String, Field> entry : fields.entrySet()) {
                String propertyName = entry.getKey();
                Field field = tryMakeAccessible(entry.getValue());
                fieldsGettersAndSetters.put(propertyName, new Object[]{field, null, null});
            }
        }
        if (propertyAccessStrategy.isGetterSetterScanAllowed()) {
            Map<String, PropertyDescriptor> properties = scanProperties(classHierarchy);
            for (Map.Entry<String, PropertyDescriptor> entry : properties.entrySet()) {
                PropertyDescriptor property = entry.getValue();
                Method getter = tryMakeAccessible(locateGetter(entityClass, property));
                Method setter = tryMakeAccessible(locateSetter(entityClass, property));
                Object[] value = fieldsGettersAndSetters.get(entry.getKey());
                if (value != null) {
                    value[1] = getter;
                    value[2] = setter;
                } else if (getter != null || setter != null) {
                    fieldsGettersAndSetters.put(entry.getKey(), new Object[]{null, getter, setter});
                }
            }
        }
        Set<MappedProperty<?>> mappedProperties = new HashSet<MappedProperty<?>>(fieldsGettersAndSetters.size());
        for (Map.Entry<String, Object[]> entry : fieldsGettersAndSetters.entrySet()) {
            String propertyName = entry.getKey();
            Field field = (Field) entry.getValue()[0];
            Method getter = (Method) entry.getValue()[1];
            Method setter = (Method) entry.getValue()[2];
            Map<Class<? extends Annotation>, Annotation> annotations = scanPropertyAnnotations(field, getter);
            AnnotationChecks.validateAnnotations(propertyName, annotations, allowed);
            if (isTransient(propertyName, field, getter, setter, annotations)) {
                LOGGER.debug(String.format("Property '%s' is transient and will not be mapped", propertyName));
                continue;
            }
            if (!annotations.containsKey(Computed.class) && field == null && getter == null) {
                throw new IllegalArgumentException(String.format("Property '%s' is not readable", propertyName));
            }
            if (field == null && setter == null) {
                throw new IllegalArgumentException(String.format("Property '%s' is not writable", propertyName));
            }
            String mappedName = inferMappedName(entityClass, propertyName, annotations);
            MappedProperty<?> property = createMappedProperty(entityClass, propertyName, mappedName, field, getter, setter, annotations);
            mappedProperties.add(property);
        }
        return mappedProperties;
    }

    /**
     * Returns {@code true} if the given property is transient,
     * {@code false} otherwise.
     * <p/>
     * If this method returns {@code true} the given property will not be mapped.
     * The implementation provided here relies on the
     * {@link #setPropertyTransienceStrategy(PropertyTransienceStrategy) transience strategy}
     * and the {@link #setTransientPropertyNames(Set) transient property names}
     * configured on this mapper.
     * <p/>
     * Subclasses may override this method to take full control of which properties
     * should be mapped and which should be considered transient.
     *
     * @param propertyName the property name; may not be {@code null}.
     * @param field        the property field; may be {@code null}.
     * @param getter       the getter method for this property; may be {@code null}.
     * @param setter       the setter method for this property; may be {@code null}.
     * @param annotations  the annotations found on this property; may be empty but never {@code null}.
     * @return {@code true} if the given property is transient (i.e., non-mapped), {@code false} otherwise.
     */
    @SuppressWarnings("unused")
    protected boolean isTransient(String propertyName, Field field, Method getter, Method setter, Map<Class<? extends Annotation>, Annotation> annotations) {
        if (propertyTransienceStrategy == PropertyTransienceStrategy.OPT_OUT)
            return annotations.containsKey(Transient.class)
                    || (transientPropertyNames.contains(propertyName)
                    && Collections.disjoint(annotations.keySet(), NON_TRANSIENT_ANNOTATIONS));
        else
            return Collections.disjoint(annotations.keySet(), NON_TRANSIENT_ANNOTATIONS);
    }

    /**
     * Locates a getter method for the given mapped class and given property.
     * <p/>
     * Most users should rely on the implementation provided here.
     * It is however possible to return any non-standard method, as long as it does
     * not take parameters, and its return type is assignable to (and covariant with) the property's type.
     * This might be particularly useful for boolean properties whose names are verbs, e.g. "{@code hasAccount}":
     * one could then return the non-standard method {@code boolean hasAccount()} as its getter.
     * <p/>
     * This method is never called if {@link PropertyAccessStrategy#isGetterSetterScanAllowed()} returns {@code false}.
     * Besides, implementors are free to return {@code null} if access to the property through reflection is not required
     * (in which case, they will likely have to provide a custom implementation of {@link MappedProperty}).
     *
     * @param mappedClass The mapped class; this is necessarily a class annotated with
     *                    either {@link Table @Table} or
     *                    {@link UDT @UDT}.
     * @param property    The property to locate a getter for; never {@code null}.
     * @return The getter method for the given base class and given property, or {@code null} if no getter was found, or reflection is not required.
     */
    protected Method locateGetter(@SuppressWarnings("unused") Class<?> mappedClass, PropertyDescriptor property) {
        return property.getReadMethod();
    }

    /**
     * Locates a setter method for the given mapped class and given property.
     * <p/>
     * Most users should rely on the implementation provided here.
     * It is however possible to return any non-standard method, as long as it accepts one single parameter type
     * that is contravariant with the property's type.
     * <p/>
     * This method is never called if {@link PropertyAccessStrategy#isGetterSetterScanAllowed()} returns {@code false}.
     * Besides, implementors are free to return {@code null} if access to the property through reflection is not required
     * (in which case, they will likely have to provide a custom implementation of {@link MappedProperty}).
     *
     * @param mappedClass The mapped class; this is necessarily a class annotated with
     *                    either {@link Table @Table} or
     *                    {@link UDT @UDT}.
     * @param property    The property to locate a setter for; never {@code null}.
     * @return The setter method for the given base class and given property, or {@code null} if no setter was found, or reflection is not required.
     */
    protected Method locateSetter(Class<?> mappedClass, PropertyDescriptor property) {
        Method setter = property.getWriteMethod();
        if (setter == null) {
            // JAVA-984: look for a "relaxed" setter, ie. a setter whose return type may be anything
            String propertyName = property.getName();
            String setterName = "set" + propertyName.substring(0, 1).toUpperCase() + propertyName.substring(1);
            try {
                Method m = mappedClass.getMethod(setterName, property.getPropertyType());
                if (!Modifier.isStatic(m.getModifiers()))
                    setter = m;
            } catch (NoSuchMethodException ignored) {
            }
        }
        return setter;
    }

    /**
     * Infers the Cassandra object name corresponding to given the property name.
     * <p/>
     * Most users should rely on the implementation provided here.
     * It relies on annotation values and ultimately resorts to the
     * {@link NamingStrategy} configured on this mapper.
     * <p/>
     * Subclasses may override this method if they need full control
     * over generating Cassandra object names.
     *
     * @param mappedClass The mapped class; this is necessarily a class annotated with
     *                    either {@link Table @Table} or
     *                    {@link UDT @UDT}.
     * @param propertyName The property name; may not be {@code null} nor empty.
     * @param annotations  The property annotations (found on its field and getter method); may not be {@code null} but can be empty.
     * @return The inferred Cassandra object name.
     */
    protected String inferMappedName(@SuppressWarnings("unused") Class<?> mappedClass, String propertyName, Map<Class<? extends Annotation>, Annotation> annotations) {
        if (annotations.containsKey(Computed.class)) {
            String expression = ((Computed) annotations.get(Computed.class)).value();
            if (expression.isEmpty())
                throw new IllegalArgumentException(String.format("Property '%s': attribute 'value' of annotation @Computed is mandatory for computed properties", propertyName));
            return expression;
        }

        // If a name is explicitly provided with @Column or @Field, use it
        boolean caseSensitive = false;
        String mappedName = null;
        if (annotations.containsKey(Column.class)) {
            Column column = (Column) annotations.get(Column.class);
            caseSensitive = column.caseSensitive();
            if (!column.name().isEmpty())
                mappedName = column.name();
        } else if (annotations.containsKey(com.datastax.driver.mapping.annotations.Field.class)) {
            com.datastax.driver.mapping.annotations.Field udtMappedField =
                    (com.datastax.driver.mapping.annotations.Field) annotations.get(com.datastax.driver.mapping.annotations.Field.class);
            caseSensitive = udtMappedField.caseSensitive();
            if (!udtMappedField.name().isEmpty())
                mappedName = udtMappedField.name();
        }
        if (mappedName != null) {
            return caseSensitive ? Metadata.quote(mappedName) : mappedName.toLowerCase();
        }

        // Otherwise delegate to the naming strategy
        mappedName = namingStrategy.toCassandraName(propertyName);
        if (mappedName == null || mappedName.isEmpty())
            throw new IllegalArgumentException(String.format("Property '%s': could not infer mapped name", propertyName));
        return Metadata.quoteIfNecessary(mappedName);
    }

    /**
     * Creates a {@link MappedProperty} instance.
     * <p>
     * Instances returned by the implementation below will use the Java reflection API to read and write values.
     * Subclasses may override this method if they are capable of accessing
     * properties without incurring the cost of reflection.
     *
     * @param mappedClass The mapped class; this is necessarily a class annotated with
     *                    either {@link Table @Table} or
     *                    {@link UDT @UDT}.
     * @param propertyName The property name; may not be {@code null} nor empty.
     * @param mappedName   The mapped name; may not be {@code null} nor empty.
     * @param field        The property field; may be {@code null}.
     * @param getter       The property getter method; may be {@code null}.
     * @param setter       The property setter method; may  be {@code null}.
     * @param annotations  The property annotations (found on its field and getter method); may not be {@code null} but can be empty.
     * @return a newly-allocated {@link MappedProperty} instance.
     */
    protected MappedProperty<?> createMappedProperty(Class<?> mappedClass, String propertyName, String mappedName, Field field, Method getter, Method setter, Map<Class<? extends Annotation>, Annotation> annotations) {
        return DefaultMappedProperty.create(mappedClass, propertyName, mappedName, field, getter, setter, annotations);
    }

    private static Map<String, Field> scanFields(List<Class<?>> classHierarchy) {
        HashMap<String, Field> fields = new HashMap<String, Field>();
        for (Class<?> clazz : classHierarchy) {
            for (Field field : clazz.getDeclaredFields()) {
                if (field.isSynthetic() || Modifier.isStatic(field.getModifiers()) || Modifier.isTransient(field.getModifiers()))
                    continue;
                // never override a more specific field masking another one declared in a superclass
                if (!fields.containsKey(field.getName()))
                    fields.put(field.getName(), field);
            }
        }
        return fields;
    }

    private static Map<String, PropertyDescriptor> scanProperties(List<Class<?>> classHierarchy) {
        Map<String, PropertyDescriptor> properties = new HashMap<String, PropertyDescriptor>();
        for (Class<?> clazz : classHierarchy) {
            // each time extract only current class properties
            BeanInfo beanInfo;
            try {
                beanInfo = Introspector.getBeanInfo(clazz, clazz.getSuperclass());
            } catch (IntrospectionException e) {
                throw Throwables.propagate(e);
            }
            for (PropertyDescriptor property : beanInfo.getPropertyDescriptors()) {
                if (!properties.containsKey(property.getName())) {
                    properties.put(property.getName(), property);
                }
            }
        }
        return properties;
    }

    private static Map<Class<? extends Annotation>, Annotation> scanPropertyAnnotations(Field field, Method getter) {
        Map<Class<? extends Annotation>, Annotation> annotations = new HashMap<Class<? extends Annotation>, Annotation>();
        // annotations on getters should have precedence over annotations on fields
        if (field != null)
            scanFieldAnnotations(field, annotations);
        if (getter != null)
            scanMethodAnnotations(getter, annotations);
        return annotations;
    }

    private static Map<Class<? extends Annotation>, Annotation> scanFieldAnnotations(Field field, Map<Class<? extends Annotation>, Annotation> annotations) {
        for (Annotation annotation : field.getAnnotations()) {
            annotations.put(annotation.annotationType(), annotation);
        }
        return annotations;
    }

    private static Map<Class<? extends Annotation>, Annotation> scanMethodAnnotations(Method method, Map<Class<? extends Annotation>, Annotation> annotations) {
        // 1. direct method annotations
        for (Annotation annotation : method.getAnnotations()) {
            annotations.put(annotation.annotationType(), annotation);
        }
        // 2. Class hierarchy: check for annotations in overridden methods in superclasses
        Class<?> getterClass = method.getDeclaringClass();
        for (Class<?> clazz = getterClass.getSuperclass(); clazz != null && !clazz.equals(Object.class); clazz = clazz.getSuperclass()) {
            maybeAddOverriddenMethodAnnotations(annotations, method, clazz);
        }
        // 3. Interfaces: check for annotations in implemented interfaces
        for (Class<?> clazz = getterClass; !clazz.equals(Object.class); clazz = clazz.getSuperclass()) {
            for (Class<?> itf : clazz.getInterfaces()) {
                maybeAddOverriddenMethodAnnotations(annotations, method, itf);
            }
        }
        return annotations;
    }

    private static void maybeAddOverriddenMethodAnnotations(Map<Class<? extends Annotation>, Annotation> annotations, Method getter, Class<?> clazz) {
        try {
            Method overriddenGetter = clazz.getDeclaredMethod(getter.getName(), (Class<?>[]) getter.getParameterTypes());
            for (Annotation annotation : overriddenGetter.getAnnotations()) {
                // do not override a more specific version of the annotation type being scanned
                if (!annotations.containsKey(annotation.annotationType()))
                    annotations.put(annotation.annotationType(), annotation);
            }
        } catch (NoSuchMethodException e) {
            //ok
        }
    }

    private static <T extends AccessibleObject> T tryMakeAccessible(T object) {
        if (object != null && !object.isAccessible()) {
            try {
                object.setAccessible(true);
            } catch (SecurityException e) {
                // ok
            }
        }
        return object;
    }

}
