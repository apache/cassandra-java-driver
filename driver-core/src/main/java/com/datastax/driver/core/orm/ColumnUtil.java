/*
 * Copyright 2012 Otávio Gonçalves de Santana (otaviojava)
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.datastax.driver.core.orm;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import com.datastax.driver.core.orm.exception.IndexProblemException;
import com.datastax.driver.core.orm.mapping.Column;
import com.datastax.driver.core.orm.mapping.ColumnFamily;
import com.datastax.driver.core.orm.mapping.CustomData;
import com.datastax.driver.core.orm.mapping.Embedded;
import com.datastax.driver.core.orm.mapping.EmbeddedKey;
import com.datastax.driver.core.orm.mapping.Enumerated;
import com.datastax.driver.core.orm.mapping.Index;
import com.datastax.driver.core.orm.mapping.Key;
import com.datastax.driver.core.orm.mapping.ListData;
import com.datastax.driver.core.orm.mapping.MapData;
import com.datastax.driver.core.orm.mapping.MappedSuperclass;
import com.datastax.driver.core.orm.mapping.SetData;



/**
 * Class Util for Column
 * 
 * @author otavio
 * 
 */
public enum ColumnUtil {
    INTANCE;

    /**
     * The integer class is used to enum how default
     */
    public static final Class<?> DEFAULT_ENUM_CLASS = Integer.class;
    /**
     * list contains the annotations to map a bean
     */
    private List<String> annotations;
    {
        annotations=new LinkedList<String>();
        annotations.add(Key.class.getName());
        annotations.add(SetData.class.getName());
        annotations.add(ListData.class.getName());
        annotations.add(MapData.class.getName());
        annotations.add(Column.class.getName());
        annotations.add(Embedded.class.getName());
        annotations.add(EmbeddedKey.class.getName());
        annotations.add(Enumerated.class.getName());
        annotations.add(Index.class.getName());
        annotations.add(CustomData.class.getName());
        Collections.sort(annotations);  
    }
    
    
    /**
     * Get The Column name from an Object
     * 
     * @param object
     *            - Class of the object viewed
     * @return The name of Column name if there are not will be return null
     */
    public String getColumnFamilyNameSchema(Class<?> object) {
        String schema = getSchemaConcat(object);
        return schema.concat(getColumnFamily(object));
    }

	private String getColumnFamily(Class<?> object) {
		ColumnFamily columnFamily = (ColumnFamily) object.getAnnotation(ColumnFamily.class);
      
        if (columnFamily != null) {
            return columnFamily.name().equals("") ? object.getSimpleName() : columnFamily.name();
        } 
        return object.getSimpleName();
	}

    private String getSchemaConcat(Class<?> class1) {
        String schema = getSchema(class1);
        if (!"".equals(schema)) {
            return schema.concat(".");
        }
        return "";
    }

    public String getSchema(Class<?> class1) {
    	ColumnFamily columnFamily = (ColumnFamily) class1.getAnnotation(ColumnFamily.class);
        if (columnFamily != null) {
            return columnFamily.specificKeyspace().equals("") ? "" : columnFamily.specificKeyspace();
        }
        return "";
    }

    /**
     * verifies that the name of the annotation is empty if you take the field
     * name
     * 
     * @param field
     *            - field for viewd
     * @return The name inside annotations or the field's name
     */
    public String getColumnName(Field field) {
        if (field.getAnnotation(Column.class) == null) {
            return field.getName();
        }
        return field.getAnnotation(Column.class).name().equals("") ? field.getName() : field.getAnnotation(Column.class).name();
    }

    /**
     * verifies that the name of the annotation is empty if you take the field
     * name
     * 
     * @param field
     * @return The name inside annotations or the field's name
     */
    public String getEnumeratedName(Field field) {
        if (isNormalField(field)) {
            return getColumnName(field);
        }
        return field.getName();
    }

    /**
     * Return the Field with the KeyValue Annotations
     * 
     * @see KeyValue
     * @param persistenceClass
     *            - Class of the object viewed
     * @return the Field if there are not will be return null
     */
    public Field getKeyField(Class<?> persistenceClass) {
        return getField(persistenceClass, Key.class);
    }

    /**
     * Return the Field with the complex key Annotations
     * 
     * @see KeyValue
     * @param persistenceClass
     *            - Class of the object viewed
     * @return the Field if there are not will be return null
     */
    public Field getKeyComplexField(Class<?> persistenceClass) {
        return getField(persistenceClass, EmbeddedKey.class);
    }

    /**
     * Return the Field with the IndexValue Annotations
     * 
     * @see Index
     * @param persistenceClass
     *            - Class of the object viewed
     * @return the Field if there are not will be return null
     */
    public Field getIndexField(Class<?> persistenceClass) {
        return getField(persistenceClass, Index.class);
    }

    /**
     * Return the Fields with the IndexValue Annotations
     * @author Dinusha Nandika
     * @see Index
     * @param persistenceClass - Class of the object viewed
     * @return the Fields if there are not will be return empty list
     */
    public List<Field> getIndexFields(Class<?> persistenceClass) {
      List<Field> indexFieldList = new ArrayList<Field>();
  	  for (Field field : persistenceClass.getDeclaredFields()) {
            if (field.getAnnotation(Index.class) != null) {
                indexFieldList.add(field);
            } else if (field.getAnnotation(Embedded.class) != null) {
          	  indexFieldList.addAll(getIndexFields(field.getType()));
            }
        }
        return indexFieldList;
    }
    
    /**
     * Get the Field of the Object from annotation if there are not return will
     * be null
     * 
     * @param object
     *            - Class of the object viewed
     * @param annotation
     * @return
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Field getField(Class object, Class annotation) {
        for (Field field : object.getDeclaredFields()) {
            if (field.getAnnotation(annotation) != null) {
                return field;
            } else if (field.getAnnotation(Embedded.class) != null) {
                return getField(field.getType(), annotation);
            }
        }
        return null;
    }

    /**
     * list the fields in the class
     * 
     * @param class1
     * @return list of the fields
     */
    public List<Field> listFields(Class<?> class1) {
        List<Field> fields = new ArrayList<Field>();
        feedFieldList(class1, fields);
        if (isMappedSuperclass(class1)) {
            feedFieldList(class1.getSuperclass(), fields);

        }
        return fields;
    }

    /**
     * feed the list com Fields
     * 
     * @param class1
     * @param fields
     */
    private void feedFieldList(Class<?> class1, List<Field> fields) {
        
        for (Field field : class1.getDeclaredFields()) {
            
            if(isColumnToPersist(field)){
                fields.add(field);
            }
            
        }
    }
    
    /**
     * verify is field has some annotations within 
     * {@link #ColumnUtil#annotations}
     * @param field
     * @return 
     */
    private boolean isColumnToPersist(Field field) {
        
        for(Annotation annotation:field.getAnnotations()){
            int result=Collections.binarySearch(annotations,annotation.annotationType().getName());
            if(result >= 0){
                return true;
            }
        }
        return false;
    }
    /**
     * verify if this is key of the column
     * 
     * @param field
     * @return
     */
    public boolean isIdField(Field field) {
        return field.getAnnotation(Key.class) != null;
    }

    /**
     * verify if this is index of the column
     * 
     * @param field
     * @return
     */
    public boolean isIndexField(Field field) {
        return field.getAnnotation(Index.class) != null;
    }


    /**
     * verify if this is secundary index of the column
     * 
     * @param field
     * @return
     */
    public boolean isSecundaryIndexField(Field field) {
        return field.getAnnotation(Index.class) != null;
    }

    /**
     * verify if this is a normal column
     * 
     * @param field
     * @return
     */
    public boolean isNormalField(Field field) {
        return field.getAnnotation(Column.class) != null;
    }

    /**
     * verify if this is a enum column
     * 
     * @param field
     * @return
     */
    public boolean isEnumField(Field field) {
        return field.getAnnotation(Enumerated.class) != null;
    }

    /**
     * verify if this is a Embedded column
     * 
     * @param field
     * @return
     */
    public boolean isEmbeddedField(Field field) {
        return field.getAnnotation(Embedded.class) != null;
    }

    /**
     * verify if this is a Embedded id column
     * 
     * @param field
     * @return
     */
    public boolean isEmbeddedIdField(Field field) {
        return field.getAnnotation(EmbeddedKey.class) != null;
    }


    /**
     * verify is exist father to persist
     * 
     * @param class1
     * @return
     */
    public boolean isMappedSuperclass(Class<?> class1) {
        return class1.getSuperclass().getAnnotation(MappedSuperclass.class) != null;
    }
    /**
     * verify if the field is a list
     * @param field
     * @return
     */
    public boolean isList(Field field){
        return field.getAnnotation(ListData.class) != null;
    }
    /**
     * verify if the field is a map
     * @param field
     * @return
     */
    public boolean isMap(Field field){
        return field.getAnnotation(MapData.class) != null;
    }
    /**
     * verify if the field is a set
     * @param field
     * @return
     */
    public boolean isSet(Field field){
        return field.getAnnotation(SetData.class) != null;
    }

    /**
     * verify if the field is custom
     * @param field
     * @return
     */
    public boolean isCustom(Field field) {
        
        return field.getAnnotation(CustomData.class) != null;
    }
    
   /**
    * get the Field for parsing <b>columnName</b> in the <b>class1</b>. if there is no such column name or if it denies access  return null.
    * @param columnName
    * @param class1
    * @return
    */
	public Field getFieldByColumnName(String columnName, Class<?> class1) {
		
		for(Field index:getIndexFields(class1)){
			if(index.getName().equals(columnName)){
				return index;
			}
		}
		
		StringBuilder erro=new StringBuilder();
		erro.append("Not found index on ").append(class1.getName());
		erro.append(" with name ").append(columnName);
	    throw new IndexProblemException(erro.toString()); 
	}
    
	
	public KeySpaceInformation getKeySpace(String keySpace,Class<?> bean){
		String keySchema=getSchema(bean);
		KeySpaceInformation key=new KeySpaceInformation();
		key.keySpace="".equals(keySchema)?keySpace:keySchema;
		key.columnFamily=getColumnFamily(bean);
	   return key ;	
	}
	
	
	
	public class KeySpaceInformation{
		private String keySpace;
		
		private String columnFamily;

		public String getKeySpace() {
			return keySpace;
		}

		public String getColumnFamily() {
			return columnFamily;
		}
		
		
	}
    
   
}