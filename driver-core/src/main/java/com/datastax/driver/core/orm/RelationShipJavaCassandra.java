package com.datastax.driver.core.orm;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.DataType.Name;
import com.datastax.driver.core.Row;

/**
 * this enum contains the relationship between CQL's type rows and type Java
 * 
 * @author otaviojava
 * @version 1.0
 */
enum RelationShipJavaCassandra {
    INSTANCE;

    /**
     * key is cql and value is Java
     */
    private Map<String, String> mapCQL;
    /**
     * key is Java and value is CQL
     */
    private Map<String, List<String>> mapJava;

    {
        mapCQL = new HashMap<String, String>();

        mapJava = new HashMap<String, List<String>>();

        for (DataType.Name name : DataType.Name.values()) {
            String cqlType = name.name().toLowerCase();
            String javaType = name.asJavaClass().getName();
            List<String> cqlvalues = mapJava.get(javaType);
            if (cqlvalues == null) {
                cqlvalues = new LinkedList<String>();
                mapJava.put(javaType, cqlvalues);
            }
            cqlvalues.add(cqlType);
            mapCQL.put(cqlType, javaType);
        }

    }

    /**
     * methods returns the equivalent type in Java from CQL type
     * 
     * @param cqlType
     *            - cqlTypeKey
     * @return the java type
     */
    public String getJavaValue(String cqlTypeKey) {
        return mapCQL.get(cqlTypeKey);
    }

    /**
     * methods returns the equivalent type in CQL from Java type
     * 
     * @param cqlType
     *            - javaTypeKey
     * @return the java type
     */
    public List<String> getCQLType(String javaTypeKey) {
        return mapJava.get(javaTypeKey);
    }

    public String getPreferenceCQLType(String javaTypeKey) {

        if (String.class.getName().equals(javaTypeKey)) {
            return DataType.Name.TEXT.name().toLowerCase();
        }

        else if (Long.class.getName().endsWith(javaTypeKey)) {
            return DataType.Name.BIGINT.name().toLowerCase();
        }

        return getCQLType(javaTypeKey).get(0);
    }

    public Object getObject(Row row, Name cqlType, String name,Class<?>... classes) {
        switch (cqlType) {
        case ASCII:
        case TEXT:
        case VARCHAR:
            return row.getString(name);
        case BIGINT:
        case COUNTER:
            return row.getLong(name);
        case BOOLEAN:
            return row.getBool(name);
        case BLOB:
        case CUSTOM:
            return row.getBytes(name);
        case DECIMAL:
            return row.getDecimal(name);
        case DOUBLE:
            return row.getDouble(name);
        case FLOAT:
            return row.getFloat(name);
        case INT:
            return row.getInt(name);
        case VARINT:
            return row.getVarint(name);
        case TIMESTAMP:
            return row.getDate(name);
        case UUID:
        case TIMEUUID:
            return row.getUUID(name);
        case LIST:
            return row.getList(name, classes[0]);
        case MAP:
            return row.getMap(name, classes[0], classes[1]);
        case SET:
            return row.getSet(name, classes[0]);
        case INET:
            return row.getInet(name);

        default:
            break;
        }

        return null;
    }
}
