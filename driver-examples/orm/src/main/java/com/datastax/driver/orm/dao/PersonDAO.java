package com.datastax.driver.orm.dao;

import com.datastax.driver.core.orm.CassandraOperationsSimple;
import com.datastax.driver.core.orm.Persistence;
import com.datastax.driver.orm.entity.Person;

public class PersonDAO extends CassandraOperationsSimple<Person, Long> {

	
	public PersonDAO(){
		persistence=CassandraFactoryUtil.INSTANCE.getPersistence();
	}
	private Persistence persistence;
	
	@Override
	protected Persistence getCassandraPersistence() {
		return persistence;
	}
	


}
