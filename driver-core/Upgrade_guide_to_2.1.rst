Upgrade Guide to 2.1.0
======================

The purpose of this guide is to detail the changes made by the version 2.1 of
the Java Driver that are relevant to an upgrade from version 2.0. This guide
only describes breaking changes of the API, added features are not covered.

Note that almost all breaking changes are compile time ones (the one exception
being the minor change to ``TableMetadata#Options#getIndexInterval``, see point
4): if you upgrade to the 2.1 version from the 2.0 one and your application
compile, you can ignore this guide.


Breaking API Changes
--------------------

1. The ``serialize`` and ``deserialize`` methods in ``DataType`` now take an
   additional parameter: the protocol version. As explained in the javadoc,
   if unsure, the proper value to use for this parameter is the protocol version
   in use by the driver, i.e. the value returned by
   ``cluster.getConfiguration().getProtocolOptions().getProtocolVersion()``.

2. The ``parse`` method in ``DataType`` now returns a Java object, not a
   ``ByteBuffer``. The previous behavior can be simply obtained by calling
   the ``serialize`` method on the object returned.

3. The ``getValues`` method of ``RegularStatement`` now takes the protocol
   version in parameters. As above, the proper value if unsure is almost surely
   the protocol version in use
   (``cluster.getConfiguration().getProtocolOptions().getProtocolVersion()``).

4. The method ``getCaching`` method of ``TableMetadata#Options`` now returns a
   ``Map`` to account for changes to Cassandra 2.1. Also, the
   ``getIndexInterval`` method now return an ``Integer`` instead of an ``int``
   which will be ``null`` when connected to Cassandra 2.1 nodes.
