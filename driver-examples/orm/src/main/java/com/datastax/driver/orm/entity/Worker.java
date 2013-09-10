package com.datastax.driver.orm.entity;

import java.io.Serializable;

import com.datastax.driver.core.orm.mapping.Column;
import com.datastax.driver.core.orm.mapping.MappedSuperclass;



@MappedSuperclass
public class Worker implements Serializable {

	private static final long serialVersionUID = -5568409094833637814L;

	@Column
	private String name;
	
	@Column
	private Double salary;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Double getSalary() {
		return salary;
	}

	public void setSalary(Double salary) {
		this.salary = salary;
	} 
	
	
	
	
}
