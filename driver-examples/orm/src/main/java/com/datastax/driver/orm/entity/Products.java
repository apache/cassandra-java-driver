package com.datastax.driver.orm.entity;

import java.io.Serializable;

public class Products implements Serializable {

	private static final long serialVersionUID = 1L;

	private String nome;
	
	private Double value;
	
	private String country;

	public String getNome() {
		return nome;
	}

	public void setNome(String nome) {
		this.nome = nome;
	}

	public Double getValue() {
		return value;
	}

	public void setValue(Double value) {
		this.value = value;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}
	
	
	
}
