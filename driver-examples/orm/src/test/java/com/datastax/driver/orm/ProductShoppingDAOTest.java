package com.datastax.driver.orm;

import junit.framework.Assert;

import org.junit.Test;

import com.datastax.driver.core.orm.CassandraORMOperations;
import com.datastax.driver.orm.dao.ProductShoppingDAO;
import com.datastax.driver.orm.entity.ProductShopping;
import com.datastax.driver.orm.entity.Products;

public class ProductShoppingDAOTest {
	 private static final String ID = "notebook";
	private CassandraORMOperations<ProductShopping,String> dao = new ProductShoppingDAO();
	    
	    @Test
	    public void insertTest(){
	        ProductShopping shopping=new ProductShopping();
	        shopping.setName(ID);
	        Products products=new Products();
	        products.setNome("Dell Ubuntu");
	        products.setCountry("Brazil");
	        products.setValue(1000d);
	        shopping.setProducts(products);
	        Assert.assertTrue(dao.save(shopping)!= null);
	        
	    }
	    
	    @Test
	    public void retrieveTest(){
	        ProductShopping shopping=dao.findOne(ID);
	        Assert.assertNotNull(shopping);
	        
	    }
}
