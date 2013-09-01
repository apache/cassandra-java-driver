package com.datastax.driver.orm.dao;

import java.util.UUID;

import com.datastax.driver.core.orm.CassandraOperationsSimple;
import com.datastax.driver.core.orm.Persistence;
import com.datastax.driver.orm.entity.Drink;

public class DrinkDAO extends CassandraOperationsSimple<Drink, UUID> {

	
	public DrinkDAO(){
		persistence=CassandraFactoryUtil.INSTANCE.getPersistence();
	}
	private Persistence persistence;
	
	@Override
	protected Persistence getCassandraPersistence() {
		return persistence;
	}
	

}
