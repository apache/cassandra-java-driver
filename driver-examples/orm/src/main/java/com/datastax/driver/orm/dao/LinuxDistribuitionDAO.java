package com.datastax.driver.orm.dao;

import com.datastax.driver.core.orm.CassandraOperationsSimple;
import com.datastax.driver.core.orm.Persistence;
import com.datastax.driver.orm.entity.IdLinux;
import com.datastax.driver.orm.entity.LinuxDistribuition;

public class LinuxDistribuitionDAO  extends CassandraOperationsSimple<LinuxDistribuition, IdLinux>{
	
	public LinuxDistribuitionDAO(){
		persistence=CassandraFactoryUtil.INSTANCE.getPersistence();
	}
	
	private Persistence persistence;
	
	@Override
	protected Persistence getCassandraPersistence() {
		return persistence;
	}
}
