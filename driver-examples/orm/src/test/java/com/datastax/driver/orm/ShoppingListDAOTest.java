package com.datastax.driver.orm;

import java.util.Date;
import java.util.LinkedList;

import junit.framework.Assert;

import org.junit.Test;

import com.datastax.driver.core.orm.CassandraORMOperations;
import com.datastax.driver.orm.dao.ShoppingListDAO;
import com.datastax.driver.orm.entity.ShoppingList;

public class ShoppingListDAOTest {
private CassandraORMOperations<ShoppingList, String> persistence=new ShoppingListDAO();
    
    
    @Test
    public void insertTest() {
        
        Assert.assertTrue(persistence.save(getPolianaShoppingList())!=null);
    }
    private ShoppingList getPolianaShoppingList() {
        ShoppingList shopping=new ShoppingList();
        shopping.setDay(new Date());
        shopping.setName("Poliana");
        shopping.setFruits(new LinkedList<String>());
        shopping.getFruits().add("Orange");
        shopping.getFruits().add("beans");
        shopping.getFruits().add("rice");
        return shopping;
    }
    
    @Test
    public void retrieveTest() {
     ShoppingList list=persistence.findOne(getPolianaShoppingList().getName());
     Assert.assertNotNull(list);
    }
    @Test
    public void removeTest() {
        ShoppingList list=persistence.findOne(getPolianaShoppingList().getName());
        persistence.delete(list);
        list=persistence.findOne(getPolianaShoppingList().getName());
        Assert.assertNull(list);
    }
}
