/*
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

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.orm.ReturnValues.ReturnValue;

/**
 * class until to convert a row in cassandra to a especific object
 * 
 * @author otaviojava
 * 
 */
enum RecoveryObject {

    INTANCE;

    @SuppressWarnings("unchecked")
    public <T> List<T> recoverObjet(Class<T> bean, ResultSet resultSet) {

        List<T> listObjList = new LinkedList<T>();
        for (Row row : resultSet.all()) {
            Map<String, Definition> mapDefinition = createMapDefinition(row.getColumnDefinitions());

            Object newObjetc = createObject(bean, row, mapDefinition);
            listObjList.add((T) newObjetc);
        }

        return listObjList;
    }

    private <T> Object createObject(Class<T> bean, Row row,Map<String, Definition> mapDefinition) {
        Object newObjetc = ReflectionUtil.INSTANCE.newInstance(bean);

        for (Field field : ColumnUtil.INTANCE.listFields(bean)) {
            if (ColumnUtil.INTANCE.isEmbeddedField(field) || ColumnUtil.INTANCE.isEmbeddedIdField(field)) {
                Object value = createObject(field.getType(), row, mapDefinition);
                ReflectionUtil.INSTANCE.setMethod(newObjetc, field, value);
                continue;
            } 
            ReturnValue returnValue=ReturnValues.INSTANCE.factory(field);
            Object value = returnValue.getObject(mapDefinition, field, row);
            ReflectionUtil.INSTANCE.setMethod(newObjetc, field, value);
        }
        return newObjetc;
    }

    private Map<String, Definition> createMapDefinition(ColumnDefinitions columnDefinitions) {
        Map<String, Definition> map = new HashMap<String, ColumnDefinitions.Definition>();

        for (Definition column : columnDefinitions) {
            map.put(column.getName(), column);
        }
        return map;
    }

}
