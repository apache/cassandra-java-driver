Upgrade Guide to 2.1
====================

The purpose of this guide is to detail the changes made by version 2.1 of
the Java Driver.

2.1.0
-----

User API Changes
~~~~~~~~~~~~~~~~

1. The ``getCaching`` method of ``TableMetadata#Options`` now returns a
   ``Map`` to account for changes to Cassandra 2.1. Also, the
   ``getIndexInterval`` method now returns an ``Integer`` instead of an ``int``
   which will be ``null`` when connected to Cassandra 2.1 nodes.

2. ``BoundStatement`` variables that have not been set explicitly will no
   longer default to ``null``. Instead, all variables must be bound explicitly,
   otherwise the execution of the statement will fail (this also applies to
   statements inside of a ``BatchStatement``). For variables that map to a
   primitive Java type, a new ``setToNull`` method has been added.
   We made this change because the driver might soon distinguish between unset
   and null variables, so we don't want clients relying on the "leave unset to
   set to ``null``" behavior.


Internal API Changes
~~~~~~~~~~~~~~~~~~~~

The changes listed in this section should normally not impact end users of the
driver, but rather third-party frameworks and tools.

1. The ``serialize`` and ``deserialize`` methods in ``DataType`` now take an
   additional parameter: the protocol version. As explained in the javadoc,
   if unsure, the proper value to use for this parameter is the protocol version
   in use by the driver, i.e. the value returned by
   ``cluster.getConfiguration().getProtocolOptions().getProtocolVersion()``.

2. The ``parse`` method in ``DataType`` now returns a Java object, not a
   ``ByteBuffer``. The previous behavior can be obtained by calling the
   ``serialize`` method on the returned object.

3. The ``getValues`` method of ``RegularStatement`` now takes the protocol
   version as a parameter. As above, the proper value if unsure is almost surely
   the protocol version in use
   (``cluster.getConfiguration().getProtocolOptions().getProtocolVersion()``).


2.1.1
-----

Internal API Changes
~~~~~~~~~~~~~~~~~~~~

1. The ``ResultSet`` interface has a new ``wasApplied()`` method. This will
   only affect clients that provide their own implementation of this interface.


2.1.2
-----

2.1.2 brings important internal changes with native protocol v3 support, but
the impact on the public API has been kept as low as possible.

User API Changes
~~~~~~~~~~~~~~~~

1. The native protocol version is now modelled as an enum: ``ProtocolVersion``.
   Most public methods that take it as an argument have a backward-compatible
   version that takes an ``int`` (the exception being ``RegularStatement``,
   described below). For new code, prefer the enum version.

Internal API Changes
~~~~~~~~~~~~~~~~~~~~

1. ``RegularStatement.getValues`` now takes the protocol version as a
   ``ProtocolVersion`` instead of an ``int``. This is transparent for callers
   since there is a backward-compatible alternative, but if you happened to
   extend the class you'll need to update your implementation.

2. ``BatchStatement.setSerialConsistencyLevel`` now returns ``BatchStatement``
   instead of ``Statement``. Again, this only matters if you extended this
   class (if so, it might be a good idea to also have a covariant return in
   your child class).

3. The constructor of ``UnsupportedFeatureException`` now takes a
   ``ProtocolVersion`` as a parameter. This should impact few users, as there's
   hardly any reason to build instances of that class from client code.

New features
~~~~~~~~~~~~

These features are only active when the native protocol v3 is in use.

1. The driver now uses a single connection per host (as opposed to a pool in
   2.1.1). Most options in ``PoolingOptions`` are ignored, except for a new one
   called ``maxSimultaneousRequestsPerHostThreshold``. See the class's Javadocs
   for detailed explanations.

2. You can now provide a default timestamp with each query (but it will be
   ignored if the CQL query string already contains a ``USING TIMESTAMP``
   clause). This can be done on a per-statement basis with
   ``Statement.setDefaultTimestamp``, or automatically with a
   ``TimestampGenerator`` specified with
   ``Cluster.Builder.withTimestampGenerator`` (two implementations are
   provided: ``ThreadLocalMonotonicTimestampGenerator`` and
   ``AtomicMonotonicTimestampGenerator``). If you specify both, the statement's
   timestamp takes precedence over the generator. By default, the driver has
   the same behavior as 2.1.1 (no generator, timestamps are assigned by
   Cassandra unless ``USING TIMESTAMP`` was specified).

3. ``BatchStatement.setSerialConsistencyLevel`` no longer throws an exception,
   it will honor the serial consistency level for the batch.
