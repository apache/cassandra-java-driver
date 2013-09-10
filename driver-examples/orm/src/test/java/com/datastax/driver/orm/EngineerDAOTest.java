package com.datastax.driver.orm;

import junit.framework.Assert;

import org.junit.Test;

import com.datastax.driver.core.orm.CassandraORMOperations;
import com.datastax.driver.orm.dao.EngineerDAO;
import com.datastax.driver.orm.entity.Engineer;

public class EngineerDAOTest {
	 private CassandraORMOperations<Engineer,String> dao = new EngineerDAO();

	    @Test
	    public void persistTest() {
	        Assert.assertTrue(dao.save(getEngineer()) != null);

	    }

	    @Test
	    public void retrieveTest() {
	    	Engineer engineer=dao.findOne(getEngineer().getNickName());
	        Assert.assertNotNull(engineer);
	    }

	    @Test
	    public void retriveDadAtribute() {
	        Assert.assertNotNull(dao.findOne(getEngineer().getNickName()));
	    }

	    private Engineer getEngineer() {
	        Engineer engineer = new Engineer();
	        engineer.setNickName("lekito");
	        engineer.setEspecialization("computer");
	        engineer.setName("Alex");
	        engineer.setType("eletric");
	        engineer.setSalary(34d);
	        return engineer;
	    }
}
