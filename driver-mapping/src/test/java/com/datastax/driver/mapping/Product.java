package com.datastax.driver.mapping;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Inheritance;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(name = "product")
@Inheritance(column = "product_type", subClasses = {Phone.class, TV.class, Computer.class} )
public abstract class Product {

    @PartitionKey
    @Column(name = "product_id")
    private String productId;

    private float price;

    private String vendor;

    private String model;


    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    public String getVendor() {
        return vendor;
    }

    public void setVendor(String vendor) {
        this.vendor = vendor;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }
}
