package com.datastax.driver.mapping;

import com.datastax.driver.mapping.annotations.InheritanceValue;

@InheritanceValue("computer")
public class Computer extends Product {

    private String processor;
    private int ram;

    public String getProcessor() {
        return processor;
    }

    public void setProcessor(String processor) {
        this.processor = processor;
    }

    public int getRam() {
        return ram;
    }

    public void setRam(int ram) {
        this.ram = ram;
    }

}
