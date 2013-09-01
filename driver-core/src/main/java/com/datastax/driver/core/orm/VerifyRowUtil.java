/*
 * Copyright 2013 Otávio Gonçalves de Santana (otaviojava)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core.orm;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
/**
 * class util to verify the relationship between java type and cassandra columns type
 * @author otaviojava
 *
 */
enum VerifyRowUtil {
INTANCE;
    
    
   public VerifyRow factory(Field field){
       if (ColumnUtil.INTANCE.isEnumField(field)) {
           return new EnumVerify();
       } 
       else if(ColumnUtil.INTANCE.isList(field)){
           return new ListVerify();
       }
        else if(ColumnUtil.INTANCE.isSet(field)){
           return new SetVerify();
       }
        else if(ColumnUtil.INTANCE.isMap(field)){
            return new MapVerify();
        }else if(ColumnUtil.INTANCE.isCustom(field)){
            return new CustomVerify();
        }
           return new DefaultVerify();
   }
    

    
    
    class CustomVerify implements VerifyRow{

        @Override
        public List<String> getTypes(Field field) {
            
            return Arrays.asList(new String[]{"blob"});
        }
        
        
    }
    
    class DefaultVerify implements VerifyRow{

        @Override
        public List<String> getTypes(Field field) {
            return RelationShipJavaCassandra.INSTANCE.getCQLType(field.getType().getName());
        }
        
    }
    class MapVerify implements VerifyRow{

        @Override
        public List<String> getTypes(Field field) {
            return Arrays.asList(new String[]{"map"});
        }
         
     }
    
    class SetVerify implements VerifyRow{

        @Override
        public List<String> getTypes(Field field) {
            return Arrays.asList(new String[]{"set"});
        }
         
     }
    
     class ListVerify implements VerifyRow{

        @Override
        public List<String> getTypes(Field field) {
            return Arrays.asList(new String[]{"list"});
        }
         
     }
    class EnumVerify implements VerifyRow{

        @Override
        public List<String> getTypes(Field field) {
            return RelationShipJavaCassandra.INSTANCE.getCQLType(ColumnUtil.DEFAULT_ENUM_CLASS.getName());
        }
        
    }
    /**
     * contract to return the list of cassandra columns a determined which java type is acceptable
     * @author otaviojava
     */
    public interface VerifyRow{
        /**
         * 
         * @param field
         * @return
         */
        List<String> getTypes(Field field);
    }
}
