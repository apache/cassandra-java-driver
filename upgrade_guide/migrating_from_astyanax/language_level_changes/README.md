# Language change : from Thrift to CQL
The data model changes when using *CQL* (Cassandra Query Language).
*CQL* is providing an abstraction of the low-level data stored in *Cassandra*, in
opposition to *Thrift* that aims to expose the low-level data structure directly.
[But note that this changes with Cassandra 3’s new storage engine.](http://www.datastax.com/2015/12/storage-engine-30)

*Thrift* exposes *Keyspaces*, and these *Keyspaces* contain *Column Families*. A
*ColumnFamily* contains *Rows* in which each *Row* has a list of an arbitrary number
of column-values. With *CQL*, the data is **tabular**, *ColumnFamily* gets viewed
as a *Table*, the **Table Rows** get a **fixed and finite number of named columns**.
*Thrift*’s columns inside the *Rows* get distributed in a tabular way through the
_Table Rows_. See the following figure:

```ditaa
                                                     Thrift
             /-                                                                                          -\
             |                                                                                            |
             |  /------------\              /---------------+---------------+---------------+---------+   |
             |  |    cRED    |              |cFA0    1      |        2      |        3      |         |   |
             |  |     1      | ---------->  +---------------+---------------+---------------+ ...     |   +--> One Thrift
             |  |            |              |c1AB   'a'     |       'b'     |       'c'     |         |   |    ROW
             |  \------------/              \---------------+---------------+---------------+---------+   |
             |                                                                                            |
One Thrift   |                                                                                           -/
COLUMNFAMILY |          
             |  
             |  /------------\              /---------------+---------------+---------+
             |  |            |              |        1      |        2      |         |
             |  |     2      | ---------->  +---------------+---------------+ ...     |
             |  |            |              |       'a'     |       'b'     |         |
             |  \------------/              \---------------+---------------+---------+
             |  
             \- 


                      -----------------------------------------------------------------------


                                                     CQL

             /-                              
             |                               
             |  /--------------------+---------------------------------+-----------------------------\
             |  |       key          |              column1            |            value            |  
             |  +--------------------+---------------------------------+-----------------------------+
             |  | cRED   1           |  cFA0          1                |   c1AB      'a'             |
             |  +--------------------+---------------------------------+-----------------------------+ -\
             |  | cRED   1           |                2                |             'b'             |  +--> One CQL
   One CQL   |  +--------------------+---------------------------------+-----------------------------+ -/    ROW
   TABLE     |  | cRED   1           |                3                |             'c'             |
             |  +--------------------+---------------------------------+-----------------------------+
             |  | cRED  ...          |               ...               |             ...             |
             |  +--------------------+---------------------------------+-----------------------------+
             |  |        2           |                1                |             'a'             |
             |  +--------------------+---------------------------------+-----------------------------+
             |  |        2           |                2                |             'b'             |
             |  +--------------------+---------------------------------+-----------------------------+
             |  |       ...          |               ...               |             ...             |
             |  +--------------------+---------------------------------+-----------------------------+
             \-
```

Some of the columns of a *CQL Table* have a special role that is specifically
related to the *Cassandra* architecture. Indeed, the *Row key* of the *Thrift Row*,
becomes the *Partition Key* in the *CQL Table*, and can be composed of 1 or multiple
*CQL columns* (the key column in Figure 1). The *“Column”* part of the Column-value
component in a *Thrift Row*, becomes the *Clustering Column* in *CQL*, and can
also be composed of multiple columns (in the figure, column1 is the only column 
composing the *Clustering Column*, but there can be others if the Thrift's ColumnComparator
is a CompositeType).

Here is the basic architectural concept of *CQL*, a detailed explanation and *CQL*
examples can be found in this article : [http://www.planetcassandra.org/making-the-change-from-thrift-to-cql/](http://www.planetcassandra.org/making-the-change-from-thrift-to-cql/).
Understanding the *CQL* abstraction plays a key role in developing performing
and scaling applications.
