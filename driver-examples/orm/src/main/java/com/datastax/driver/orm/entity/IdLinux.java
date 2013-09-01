package com.datastax.driver.orm.entity;

import java.io.Serializable;

import com.datastax.driver.core.orm.mapping.Column;

public class IdLinux implements Serializable {
	  
	private static final long serialVersionUID = 1989354693489215616L;

	@Column
    private String name;
    
    @Column
    private String kernelVersion;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getKernelVersion() {
        return kernelVersion;
    }

    public void setKernelVersion(String kernelVersion) {
        this.kernelVersion = kernelVersion;
    }
    
}
