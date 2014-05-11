package com.datastax.driver.core.schemabuilder;

import java.util.Arrays;
import java.util.List;
import com.google.common.base.Strings;

public abstract class SchemaStatement {

    static final String NEW_LINE = "\n";
    static final String TAB = "\t";
    static final String COLUMN_FORMATTING = ",\n\t\t";
    static final String OPEN_PAREN = "(";
    static final String CLOSE_PAREN = ")";
    static final String OPEN_TYPE = "<";
    static final String CLOSE_TYPE = ">";
    static final String SPACE = " ";
    static final String SEPARATOR = ", ";
    static final String COMMA = ",";
    static final String DOT = ".";
    static final String STATIC = "static";
    static final String CREATE_TABLE = "CREATE TABLE";
    static final String IF_NOT_EXISTS = "IF NOT EXISTS";
    static final String PRIMARY_KEY = "PRIMARY KEY";

    static final String ALTER_TABLE = "ALTER TABLE";
    static final String ALTER = "ALTER";
    static final String TYPE = "TYPE";
    static final String ADD = "ADD";
    static final String RENAME = "RENAME";
    static final String TO = "TO";
    static final String WITH = "WITH";
    static final String DROP = "DROP";

    static final String DROP_TABLE = "DROP TABLE";
    static final String IF_EXISTS = "IF EXISTS";


    static final String CLUSTERING_ORDER_BY = "CLUSTERING ORDER BY";
    static final String COMPACT_STORAGE = "COMPACT STORAGE";

    static final String CREATE_INDEX = "CREATE INDEX";
    static final String ON = "ON";


    static final List<String> RESERVED_KEYWORDS = Arrays.asList("add,allow,alter,and,any,apply,asc,authorize,batch,begin,by,columnfamily,create,delete,desc,drop,each_quorum,from,grant,in,index,inet,infinity,insert,into,keyspace,keyspaces,limit,local_one,local_quorum,modify,nan,norecursive,of,on,order,password,primary,quorum,rename,revoke,schema,select,set,table,three,to,token,truncate,two,unlogged,update,use,using,where,with".split(","));

    abstract String buildInternal();

    protected static void validateNotEmpty(String columnName, String label) {
        if (Strings.isNullOrEmpty(columnName)) {
            throw new IllegalArgumentException(label + " should not be null or blank");
        }
    }

    protected static void validateNotNull(Object value, String label) {
        if (value == null) {
            throw new IllegalArgumentException(label + " should not be null");
        }
    }

    protected static void validateNotKeyWord(String label, String message) {
        if (RESERVED_KEYWORDS.contains(label)) {
            throw new IllegalArgumentException(message);
        }
    }
}
