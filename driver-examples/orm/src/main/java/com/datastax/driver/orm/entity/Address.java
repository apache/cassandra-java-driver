package com.datastax.driver.orm.entity;

import java.io.Serializable;
//import java.util.Objects;

import org.apache.commons.lang.StringUtils;

import com.datastax.driver.core.orm.mapping.Column;

public class Address implements Serializable {
    private static final long serialVersionUID = 1L;
    
    @Column(name="state")
    private String state;
    
    @Column(name="cyte")
    private String city;
    
    @Column(name="street")
    private String street;
    
    @Column(name="cep")
    private String cep;

    public String getCep() {
        return cep;
    }

    public void setCep(String cep) {
        this.cep = cep;
    }

    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Address other = (Address) obj;
//        if (!Objects.equals(this.city, other.city)) {
//            return false;
//        }
//        if (!Objects.equals(this.cep, other.cep)) {
//            return false;
//        }
        if(!StringUtils.equals(this.city, other.city)){
        	return false;
        }
        if(!StringUtils.equals(this.cep, other.cep)){
        	return false;
        }
        return true;
    }

    public int hashCode() {
        int hash = 7;
        hash = 97 * hash + this.city.hashCode();
        hash = 97 * hash + cep.hashCode();
        return hash;
    }
    
    
    
    
}
