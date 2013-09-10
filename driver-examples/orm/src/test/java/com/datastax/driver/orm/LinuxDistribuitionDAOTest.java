package com.datastax.driver.orm;

import junit.framework.Assert;

import org.junit.Test;

import com.datastax.driver.core.orm.CassandraORMOperations;
import com.datastax.driver.orm.dao.LinuxDistribuitionDAO;
import com.datastax.driver.orm.entity.IdLinux;
import com.datastax.driver.orm.entity.LinuxDistribuition;

public class LinuxDistribuitionDAOTest {

    private CassandraORMOperations<LinuxDistribuition,IdLinux> linuxDAO=new LinuxDistribuitionDAO();
    
    @Test
    public void insertTest() {
        LinuxDistribuition linux = getUbuntu();
        Assert.assertTrue(linuxDAO.save(linux) != null);
    }


    @Test
    public void removeTest() {
        LinuxDistribuition ekaaty=getEkaaty();
        linuxDAO.save(ekaaty);
        linuxDAO.delete(ekaaty);
        Assert.assertNull(linuxDAO.findOne(ekaaty.getId()));
    }
    
    
    
    @Test
    public void retrieveTest() {
        LinuxDistribuition ubuntu=getUbuntu();
        LinuxDistribuition linux = linuxDAO.findOne(ubuntu.getId());
        Assert.assertTrue(linux.getVersion().equals(ubuntu.getVersion()));
    }
    
    private LinuxDistribuition getUbuntu() {
        LinuxDistribuition linux=new LinuxDistribuition();
        linux.setId(new IdLinux());
        linux.getId().setKernelVersion("3.5");
        linux.getId().setName("Ubuntu");
        linux.setGuy("Unity");
        linux.setDescriptions("The most popular linux distributions around world");
        linux.setVersion("12.04");
        return linux;
    }
    
    private LinuxDistribuition getEkaaty() {
        LinuxDistribuition linux=new LinuxDistribuition();
        linux.setId(new IdLinux());
        linux.getId().setKernelVersion("3.5");
        linux.getId().setName("Ekaaty");
        linux.setGuy("KDE");
        linux.setDescriptions("The brazilian distribution, made by Brazilians by Brazilians and for Brazilians");
        linux.setVersion("5 Pataxós");
        return linux;
    }
}
