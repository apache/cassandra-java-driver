package com.datastax.driver.orm.entity;

import com.datastax.driver.core.orm.mapping.ColumnFamily;
import com.datastax.driver.core.orm.mapping.CustomData;
import com.datastax.driver.core.orm.mapping.Key;

@ColumnFamily(name="product")
public class ProductShopping {

    @Key
    private String name;
    
    @CustomData(classCustmo=ProductsCustomData.class)
    private Products products;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Products getProducts() {
        return products;
    }

    public void setProducts(Products products) {
        this.products = products;
    }
    
    
}
