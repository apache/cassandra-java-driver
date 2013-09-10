package com.datastax.driver.orm;

import java.util.UUID;

import junit.framework.Assert;

import org.junit.Test;

import com.datastax.driver.orm.dao.DrinkDAO;
import com.datastax.driver.orm.entity.Drink;

public class DrinkDAOTest {
	
	private DrinkDAO dao=new DrinkDAO();
	private UUID uuid=UUID.fromString("ff1fa232-280e-4cca-bad8-86eccf8e7299"); 

	    @Test
	    public void insertTest() {
	      Drink drink=new Drink();
	      drink.setId(uuid);
	      drink.setFlavor("orange");
	      drink.setName("cup of juice");
	    
	      Assert.assertTrue(dao.save(drink)!=null);
	    }
	    
	    
	    @Test
	    public void retrieveTest() {
	    	Drink drink = dao.findOne(uuid);
	        Assert.assertNotNull(drink);
	    }
}
