package org.apache.cassandra.cql;

import java.io.Serializable;
import java.util.List;

public class TestClass implements Serializable
{
    private static final long serialVersionUID = 1L;
    String s;
    Integer i;
    List<String>  l;
    
    public TestClass(String s, Integer i, List<String>l)
    {
        this.s = s;
        this.i = i;
        this.l = l;
    }
    public String toString()
    {
        return String.format("TestClass(%s, %d, %s)", s,i,l);
    }
}
