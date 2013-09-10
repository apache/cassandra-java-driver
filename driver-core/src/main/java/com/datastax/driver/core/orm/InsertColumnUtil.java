package com.datastax.driver.core.orm;

import java.lang.reflect.Field;

import com.datastax.driver.core.orm.mapping.CustomData;
import com.datastax.driver.core.orm.mapping.Customizable;

enum InsertColumnUtil  {
INSTANCE;

public InsertColumn factory(Field field){
    if (ColumnUtil.INTANCE.isEnumField(field)) {
        return new EnumInsert();
    }
    if(ColumnUtil.INTANCE.isCustom(field)){
        return new CustomInsert();
    }
    
    return new DefaultInsert();
}


class CustomInsert implements InsertColumn{

    @Override
    public Object getObject(Object bean, Field field) {
       CustomData customData=field.getAnnotation(CustomData.class);
       Customizable customizable=(Customizable) ReflectionUtil.INSTANCE.newInstance(customData.classCustmo());
       return customizable.read(ReflectionUtil.INSTANCE.getMethod(bean, field));
    }
    
    
}

class EnumInsert implements InsertColumn{

    @Override
    public Object getObject(Object bean, Field field) {
        Enum<?> enumS = (Enum<?>) ReflectionUtil.INSTANCE.getMethod(bean,field);
        return enumS.ordinal();
    }
    
}
class DefaultInsert implements InsertColumn{

    @Override
    public Object getObject(Object bean, Field field) {
        return ReflectionUtil.INSTANCE.getMethod(bean, field);
    }
    
}

public interface InsertColumn{
    
    Object getObject(Object bean,Field field);
    
}
}
