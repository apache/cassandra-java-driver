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
package com.datastax.oss.driver.internal.mapper.processor.util.generation;

import com.datastax.oss.driver.api.core.data.GettableByName;
import com.datastax.oss.driver.api.core.data.SettableByName;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;

/**
 * Wraps the declared type of an entity property (or DAO method parameter) that will be injected
 * into a {@link SettableByName}, or extracted from a {@link GettableByName}.
 *
 * <p>The goal is to detect if the type contains other mapped entities, that must be translated into
 * UDT values.
 */
public abstract class PropertyType {

  private static final ClassName UDT_VALUE_CLASS_NAME = ClassName.get(UdtValue.class);
  public static final ClassName LIST_CLASS_NAME = ClassName.get(List.class);
  public static final ClassName SET_CLASS_NAME = ClassName.get(Set.class);
  public static final ClassName MAP_CLASS_NAME = ClassName.get(Map.class);

  public static PropertyType parse(TypeMirror typeMirror, ProcessorContext context) {
    if (typeMirror.getKind() == TypeKind.DECLARED) {
      DeclaredType declaredType = (DeclaredType) typeMirror;
      if (declaredType.asElement().getAnnotation(Entity.class) != null) {
        return new SingleEntity(declaredType);
      } else if (context.getClassUtils().isList(declaredType)) {
        PropertyType elementType = parse(declaredType.getTypeArguments().get(0), context);
        return (elementType instanceof Simple)
            ? new Simple(typeMirror)
            : new EntityList(typeMirror, elementType);
      } else if (context.getClassUtils().isSet(declaredType)) {
        PropertyType elementType = parse(declaredType.getTypeArguments().get(0), context);
        return (elementType instanceof Simple)
            ? new Simple(typeMirror)
            : new EntitySet(typeMirror, elementType);
      } else if (context.getClassUtils().isMap(declaredType)) {
        PropertyType keyType = parse(declaredType.getTypeArguments().get(0), context);
        PropertyType valueType = parse(declaredType.getTypeArguments().get(1), context);
        return (keyType instanceof Simple && valueType instanceof Simple)
            ? new Simple(typeMirror)
            : new EntityMap(typeMirror, keyType, valueType);
      }
    }
    return new Simple(typeMirror);
  }

  private final TypeMirror typeMirror;

  protected PropertyType(TypeMirror typeMirror) {
    this.typeMirror = typeMirror;
  }

  public TypeMirror asTypeMirror() {
    return typeMirror;
  }

  public abstract TypeName asTypeName();

  /**
   * Returns the name of the type we will convert to before saving to the database; that is,
   * replacing every entity class by {@code UdtValue}.
   */
  public abstract TypeName asRawTypeName();

  /**
   * A type that does not contain any mapped entity.
   *
   * <p>Note that it can still be a collection, for example {@code Map<String, List<Integer>>}.
   */
  public static class Simple extends PropertyType {
    public final TypeName typeName;

    public Simple(TypeMirror typeMirror) {
      super(typeMirror);
      this.typeName = ClassName.get(typeMirror);
    }

    @Override
    public TypeName asTypeName() {
      return typeName;
    }

    @Override
    public TypeName asRawTypeName() {
      return typeName;
    }
  }

  /** A mapped entity. */
  public static class SingleEntity extends PropertyType {
    public final ClassName entityName;

    public SingleEntity(DeclaredType declaredType) {
      super(declaredType);
      this.entityName = (ClassName) TypeName.get(declaredType);
    }

    @Override
    public TypeName asTypeName() {
      return entityName;
    }

    @Override
    public TypeName asRawTypeName() {
      return UDT_VALUE_CLASS_NAME;
    }
  }

  /** A list of another non-simple type. */
  public static class EntityList extends PropertyType {
    public final PropertyType elementType;

    public EntityList(TypeMirror typeMirror, PropertyType elementType) {
      super(typeMirror);
      this.elementType = elementType;
    }

    @Override
    public TypeName asTypeName() {
      return ParameterizedTypeName.get(LIST_CLASS_NAME, elementType.asTypeName());
    }

    @Override
    public TypeName asRawTypeName() {
      return ParameterizedTypeName.get(LIST_CLASS_NAME, elementType.asRawTypeName());
    }
  }

  /** A set of another non-simple type. */
  public static class EntitySet extends PropertyType {
    public final PropertyType elementType;

    public EntitySet(TypeMirror typeMirror, PropertyType elementType) {
      super(typeMirror);
      this.elementType = elementType;
    }

    @Override
    public TypeName asTypeName() {
      return ParameterizedTypeName.get(SET_CLASS_NAME, elementType.asTypeName());
    }

    @Override
    public TypeName asRawTypeName() {
      return ParameterizedTypeName.get(SET_CLASS_NAME, elementType.asRawTypeName());
    }
  }

  /** A map where either the key type, the value type, or both, are non-simple types. */
  public static class EntityMap extends PropertyType {
    public final PropertyType keyType;
    public final PropertyType valueType;

    public EntityMap(TypeMirror typeMirror, PropertyType keyType, PropertyType valueType) {
      super(typeMirror);
      this.keyType = keyType;
      this.valueType = valueType;
    }

    @Override
    public TypeName asTypeName() {
      return ParameterizedTypeName.get(
          MAP_CLASS_NAME, keyType.asTypeName(), valueType.asTypeName());
    }

    @Override
    public TypeName asRawTypeName() {
      return ParameterizedTypeName.get(
          MAP_CLASS_NAME, keyType.asRawTypeName(), valueType.asRawTypeName());
    }
  }
}
