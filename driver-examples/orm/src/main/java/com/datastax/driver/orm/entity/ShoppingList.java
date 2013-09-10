package com.datastax.driver.orm.entity;

import java.util.Date;
import java.util.List;

import com.datastax.driver.core.orm.mapping.Column;
import com.datastax.driver.core.orm.mapping.ColumnFamily;
import com.datastax.driver.core.orm.mapping.Key;
import com.datastax.driver.core.orm.mapping.ListData;

@ColumnFamily(name="shopping")
public class ShoppingList {

    @Key
    private String name;
    
    @Column(name="day")
    private Date day;
    
    @ListData
    @Column(name="frutis")
    private List<String> fruits;
    
    @Column(name="storeName")
    private String storeName;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Date getDay() {
        return day;
    }

    public void setDay(Date day) {
        this.day = day;
    }

    public List<String> getFruits() {
        return fruits;
    }

    public void setFruits(List<String> fruits) {
        this.fruits = fruits;
    }

    public String getStoreName() {
        return storeName;
    }

    public void setStoreName(String storeName) {
        this.storeName = storeName;
    }
    
    
    
    
    
    
}
