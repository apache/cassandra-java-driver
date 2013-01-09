package com.datastax.driver.mapping;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.InheritanceValue;

@InheritanceValue("phone")
public class Phone extends Product {

    @Column(name = "screen_size")
    private float screenSize;

    @Column(name = "os")
    private OperatingSystem operatingSystem;

    public float getScreenSize() {
        return screenSize;
    }

    public void setScreenSize(float screenSize) {
        this.screenSize = screenSize;
    }

    public OperatingSystem getOperatingSystem() {
        return operatingSystem;
    }

    public void setOperatingSystem(OperatingSystem operatingSystem) {
        this.operatingSystem = operatingSystem;
    }

}
