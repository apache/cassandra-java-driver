/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.mapping;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.mapping.Mapper.Option.SaveNullFields;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Computed;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import static com.datastax.driver.mapping.Mapper.Option.Type.SAVE_NULL_FIELDS;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * An object handling the mapping of a particular class.
 * <p/>
 * A {@code Mapper} object is obtained from a {@code MappingManager} using the
 * {@link MappingManager#mapper} method.
 */
public class Mapper<T> {

    private static final Logger logger = LoggerFactory.getLogger(Mapper.class);
    private static final Function<Object, Void> TO_NULL = Functions.constant(null);

    final MappingManager manager;
    final ProtocolVersion protocolVersion;
    final Class<T> klass;
    final EntityMapper<T> mapper;
    final TableMetadata tableMetadata;

    // Cache prepared statements for each type of query we use.
    private final ConcurrentMap<MapperQueryKey, ListenableFuture<PreparedStatement>> preparedQueries = new ConcurrentHashMap<MapperQueryKey, ListenableFuture<PreparedStatement>>();

    private volatile EnumMap<Option.Type, Option> defaultSaveOptions;
    private volatile EnumMap<Option.Type, Option> defaultGetOptions;
    private volatile EnumMap<Option.Type, Option> defaultDeleteOptions;

    private static final EnumMap<Option.Type, Option> NO_OPTIONS = new EnumMap<Option.Type, Option>(Option.Type.class);

    final Function<ResultSet, T> mapOneFunction;
    final Function<ResultSet, T> mapOneFunctionWithoutAliases;
    final Function<ResultSet, Result<T>> mapAllFunctionWithoutAliases;

    Mapper(MappingManager manager, Class<T> klass, EntityMapper<T> mapper) {
        this.manager = manager;
        this.klass = klass;
        this.mapper = mapper;

        KeyspaceMetadata keyspace = session().getCluster().getMetadata().getKeyspace(mapper.getKeyspace());
        this.tableMetadata = keyspace == null ? null : keyspace.getTable(mapper.getTable());

        this.protocolVersion = manager.getSession().getCluster().getConfiguration().getProtocolOptions().getProtocolVersion();
        this.mapOneFunction = new Function<ResultSet, T>() {
            @Override
            public T apply(ResultSet rs) {
                return Mapper.this.map(rs).one();
            }
        };
        this.mapOneFunctionWithoutAliases = new Function<ResultSet, T>() {
            @Override
            public T apply(ResultSet rs) {
                return Mapper.this.map(rs).one();
            }
        };
        this.mapAllFunctionWithoutAliases = new Function<ResultSet, Result<T>>() {
            @Override
            public Result<T> apply(ResultSet rs) {
                return Mapper.this.map(rs);
            }
        };

        this.defaultSaveOptions = NO_OPTIONS;
        this.defaultGetOptions = NO_OPTIONS;
        this.defaultDeleteOptions = NO_OPTIONS;
    }

    Session session() {
        return manager.getSession();
    }

    ListenableFuture<PreparedStatement> getPreparedQueryAsync(QueryType type, Set<ColumnMapper<?>> columns, EnumMap<Option.Type, Option> options) {

        final MapperQueryKey pqk = new MapperQueryKey(type, columns, options);
        ListenableFuture<PreparedStatement> existingFuture = preparedQueries.get(pqk);
        if (existingFuture == null) {
            final SettableFuture<PreparedStatement> future = SettableFuture.create();
            ListenableFuture<PreparedStatement> old = preparedQueries.putIfAbsent(pqk, future);
            if (old != null) {
                return old;
            } else {
                String queryString = type.makePreparedQueryString(tableMetadata, mapper, manager, columns, options.values());
                logger.debug("Preparing query {}", queryString);
                SimpleStatement s = new SimpleStatement(queryString);
                // all queries generated by the mapper are idempotent
                s.setIdempotent(true);
                Futures.addCallback(session().prepareAsync(s), new FutureCallback<PreparedStatement>() {
                    @Override
                    public void onSuccess(PreparedStatement stmt) {
                        future.set(stmt);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        future.setException(t);
                    }
                });
                return future;
            }
        } else {
            return existingFuture;
        }
    }

    ListenableFuture<PreparedStatement> getPreparedQueryAsync(QueryType type, EnumMap<Option.Type, Option> options) {
        return getPreparedQueryAsync(type, Collections.<ColumnMapper<?>>emptySet(), options);
    }

    Class<T> getMappedClass() {
        return klass;
    }

    /**
     * The {@code TableMetadata} for this mapper.
     *
     * @return the {@code TableMetadata} for this mapper or {@code null} if keyspace is not set.
     */
    public TableMetadata getTableMetadata() {
        return tableMetadata;
    }

    /**
     * The {@code MappingManager} managing this mapper.
     *
     * @return the {@code MappingManager} managing this mapper.
     */
    public MappingManager getManager() {
        return manager;
    }

    /**
     * Creates a query that can be used to save the provided entity.
     * <p/>
     * This method is useful if you want to setup a number of options (tracing,
     * conistency level, ...) of the returned statement before executing it manually
     * or need access to the {@code ResultSet} object after execution (to get the
     * trace, the execution info, ...), but in other cases, calling {@link #save}
     * or {@link #saveAsync} is shorter.
     *
     * @param entity the entity to save.
     * @return a query that saves {@code entity} (based on it's defined mapping).
     */
    public Statement saveQuery(T entity) {
        try {
            return Uninterruptibles.getUninterruptibly(saveQueryAsync(entity, this.defaultSaveOptions));
        } catch (ExecutionException e) {
            throw DriverThrowables.propagateCause(e);
        }
    }

    /**
     * Creates a query that can be used to save the provided entity.
     * <p/>
     * This method is useful if you want to setup a number of options (tracing,
     * conistency level, ...) of the returned statement before executing it manually
     * or need access to the {@code ResultSet} object after execution (to get the
     * trace, the execution info, ...), but in other cases, calling {@link #save}
     * or {@link #saveAsync} is shorter.
     * This method allows you to provide a suite of {@link Option} to include in
     * the SAVE query. Options currently supported for SAVE are :
     * <ul>
     * <li>Timestamp</li>
     * <li>Time-to-live (ttl)</li>
     * <li>Consistency level</li>
     * <li>Tracing</li>
     * </ul>
     *
     * @param entity the entity to save.
     * @return a query that saves {@code entity} (based on it's defined mapping).
     */
    public Statement saveQuery(T entity, Option... options) {
        try {
            return Uninterruptibles.getUninterruptibly(saveQueryAsync(entity, toMapWithDefaults(options, this.defaultSaveOptions)));
        } catch (ExecutionException e) {
            throw DriverThrowables.propagateCause(e);
        }
    }

    private ListenableFuture<BoundStatement> saveQueryAsync(T entity, final EnumMap<Option.Type, Option> options) {
        final Map<ColumnMapper<?>, Object> values = new HashMap<ColumnMapper<?>, Object>();
        boolean saveNullFields = shouldSaveNullFields(options);

        for (ColumnMapper<T> cm : mapper.allColumns()) {
            Object value = cm.getValue(entity);
            if (cm.kind != ColumnMapper.Kind.COMPUTED && (saveNullFields || value != null)) {
                values.put(cm, value);
            }
        }

        return Futures.transform(getPreparedQueryAsync(QueryType.SAVE, values.keySet(), options), new Function<PreparedStatement, BoundStatement>() {
            @Override
            public BoundStatement apply(PreparedStatement input) {
                BoundStatement bs = input.bind();
                int i = 0;
                for (Map.Entry<ColumnMapper<?>, Object> entry : values.entrySet()) {
                    ColumnMapper<?> mapper = entry.getKey();
                    Object value = entry.getValue();
                    setObject(bs, i++, value, mapper);
                }

                if (mapper.writeConsistency != null)
                    bs.setConsistencyLevel(mapper.writeConsistency);

                for (Option opt : options.values()) {
                    opt.checkValidFor(QueryType.SAVE, manager);
                    opt.addToPreparedStatement(bs, i++);
                }

                return bs;
            }
        });
    }

    private static boolean shouldSaveNullFields(EnumMap<Option.Type, Option> options) {
        SaveNullFields option = (SaveNullFields) options.get(SAVE_NULL_FIELDS);
        return option == null || option.saveNullFields;
    }

    private static void setObject(BoundStatement bs, int i, Object value, ColumnMapper<?> mapper) {
        TypeCodec<Object> customCodec = mapper.getCustomCodec();
        if (customCodec != null)
            bs.set(i, value, customCodec);
        else
            bs.set(i, value, mapper.getJavaType());
    }

    /**
     * Saves an entity mapped by this mapper.
     * <p/>
     * This method is basically equivalent to: {@code getManager().getSession().execute(saveQuery(entity))}.
     *
     * @param entity the entity to save.
     */
    public void save(T entity) {
        try {
            Uninterruptibles.getUninterruptibly(saveAsync(entity));
        } catch (ExecutionException e) {
            throw DriverThrowables.propagateCause(e);
        }
    }

    /**
     * Saves an entity mapped by this mapper and using special options for save.
     * This method allows you to provide a suite of {@link Option} to include in
     * the SAVE query. Options currently supported for SAVE are :
     * <ul>
     * <li>Timestamp</li>
     * <li>Time-to-live (ttl)</li>
     * <li>Consistency level</li>
     * <li>Tracing</li>
     * </ul>
     *
     * @param entity  the entity to save.
     * @param options the options object specified defining special options when saving.
     */
    public void save(T entity, Option... options) {
        try {
            Uninterruptibles.getUninterruptibly(saveAsync(entity, options));
        } catch (ExecutionException e) {
            throw DriverThrowables.propagateCause(e);
        }
    }

    /**
     * Saves an entity mapped by this mapper asynchronously.
     * <p/>
     * This method is basically equivalent to: {@code getManager().getSession().executeAsync(saveQuery(entity))}.
     *
     * @param entity the entity to save.
     * @return a future on the completion of the save operation.
     */
    public ListenableFuture<Void> saveAsync(T entity) {
        return submitVoidQueryAsync(saveQueryAsync(entity, this.defaultSaveOptions));
    }

    /**
     * Save an entity mapped by this mapper asynchronously using special options for save.
     * <p/>
     * This method is basically equivalent to: {@code getManager().getSession().executeAsync(saveQuery(entity, options))}.
     *
     * @param entity  the entity to save.
     * @param options the options object specified defining special options when saving.
     * @return a future on the completion of the save operation.
     */
    public ListenableFuture<Void> saveAsync(T entity, Option... options) {
        return submitVoidQueryAsync(saveQueryAsync(entity, toMapWithDefaults(options, this.defaultSaveOptions)));
    }

    private ListenableFuture<Void> submitVoidQueryAsync(ListenableFuture<BoundStatement> bsFuture) {
        ListenableFuture<ResultSet> rsFuture = Futures.transform(bsFuture, new AsyncFunction<BoundStatement, ResultSet>() {
            @Override
            public ListenableFuture<ResultSet> apply(BoundStatement bs) throws Exception {
                return session().executeAsync(bs);
            }
        });
        return Futures.transform(rsFuture, TO_NULL);
    }

    /**
     * Creates a query to fetch entity given its PRIMARY KEY.
     * <p/>
     * The values provided must correspond to the columns composing the PRIMARY
     * KEY (in the order of said primary key).
     * <p/>
     * This method is useful if you want to setup a number of options (tracing,
     * conistency level, ...) of the returned statement before executing it manually,
     * but in other cases, calling {@link #get} or {@link #getAsync} is shorter.
     * <p/>
     * This method allows you to provide a suite of {@link Option} to include in
     * the GET query. Options currently supported for GET are :
     * <ul>
     * <li>Consistency level</li>
     * <li>Tracing</li>
     * </ul>
     *
     * @param objects the primary key of the entity to fetch, or more precisely
     *                the values for the columns of said primary key in the order of the primary key.
     *                Can be followed by {@link Option} to include in the DELETE query.
     * @return a query that fetch the entity of PRIMARY KEY {@code objects}.
     * @throws IllegalArgumentException if the number of value provided differ from
     *                                  the number of columns composing the PRIMARY KEY of the mapped class, or if
     *                                  at least one of those values is {@code null}.
     */
    public Statement getQuery(Object... objects) {
        try {
            return Uninterruptibles.getUninterruptibly(getQueryAsync(objects));
        } catch (ExecutionException e) {
            throw DriverThrowables.propagateCause(e);
        }
    }

    private ListenableFuture<BoundStatement> getQueryAsync(Object... objects) {
        // Order and duplicates matter for primary keys
        List<Object> pks = new ArrayList<Object>();
        EnumMap<Option.Type, Option> options = new EnumMap<Option.Type, Option>(defaultGetOptions);
        for (Object o : objects) {
            if (o instanceof Option) {
                Option option = (Option) o;
                options.put(option.type, option);
            } else {
                pks.add(o);
            }
        }
        return getQueryAsync(pks, options);
    }

    private ListenableFuture<BoundStatement> getQueryAsync(final List<Object> primaryKeys, final EnumMap<Option.Type, Option> options) {
        if (primaryKeys.size() != mapper.primaryKeySize())
            throw new IllegalArgumentException(String.format("Invalid number of PRIMARY KEY columns provided, %d expected but got %d", mapper.primaryKeySize(), primaryKeys.size()));

        return Futures.transform(getPreparedQueryAsync(QueryType.GET, options), new Function<PreparedStatement, BoundStatement>() {
            @Override
            public BoundStatement apply(PreparedStatement input) {
                BoundStatement bs = new MapperBoundStatement(input);
                int i = 0;
                for (Object value : primaryKeys) {
                    ColumnMapper<T> column = mapper.getPrimaryKeyColumn(i);
                    if (value == null) {
                        throw new IllegalArgumentException(String.format("Invalid null value for PRIMARY KEY column %s (argument %d)", column.getColumnName(), i));
                    }
                    setObject(bs, i++, value, column);
                }

                if (mapper.readConsistency != null)
                    bs.setConsistencyLevel(mapper.readConsistency);

                for (Option opt : options.values()) {
                    opt.checkValidFor(QueryType.GET, manager);
                    opt.addToPreparedStatement(bs, i);
                    if (opt.isIncludedInQuery())
                        i++;
                }
                return bs;
            }
        });
    }

    /**
     * Fetch an entity based on its primary key.
     * <p/>
     * This method is basically equivalent to: {@code map(getManager().getSession().execute(getQuery(objects))).one()}.
     *
     * @param objects the primary key of the entity to fetch, or more precisely
     *                the values for the columns of said primary key in the order of the primary key.
     *                Can be followed by {@link Option} to include in the DELETE query.
     * @return the entity fetched or {@code null} if it doesn't exist.
     * @throws IllegalArgumentException if the number of value provided differ from
     *                                  the number of columns composing the PRIMARY KEY of the mapped class, or if
     *                                  at least one of those values is {@code null}.
     */
    public T get(Object... objects) {
        try {
            return Uninterruptibles.getUninterruptibly(getAsync(objects));
        } catch (ExecutionException e) {
            throw DriverThrowables.propagateCause(e);
        }
    }

    /**
     * Fetch an entity based on its primary key asynchronously.
     * <p/>
     * This method is basically equivalent to mapping the result of: {@code getManager().getSession().executeAsync(getQuery(objects))}.
     *
     * @param objects the primary key of the entity to fetch, or more precisely
     *                the values for the columns of said primary key in the order of the primary key.
     *                Can be followed by {@link Option} to include in the DELETE query.
     * @return a future on the fetched entity. The return future will yield
     * {@code null} if said entity doesn't exist.
     * @throws IllegalArgumentException if the number of value provided differ from
     *                                  the number of columns composing the PRIMARY KEY of the mapped class, or if
     *                                  at least one of those values is {@code null}.
     */
    public ListenableFuture<T> getAsync(final Object... objects) {
        ListenableFuture<BoundStatement> bsFuture = getQueryAsync(objects);
        ListenableFuture<ResultSet> rsFuture = Futures.transform(bsFuture, new AsyncFunction<BoundStatement, ResultSet>() {
            @Override
            public ListenableFuture<ResultSet> apply(BoundStatement bs) throws Exception {
                return session().executeAsync(bs);
            }
        });
        return Futures.transform(rsFuture, mapOneFunction);
    }

    /**
     * Creates a query that can be used to delete the provided entity.
     * <p/>
     * This method is a shortcut that extract the PRIMARY KEY from the
     * provided entity and call {@link #deleteQuery(Object...)} with it.
     * This method allows you to provide a suite of {@link Option} to include in
     * the DELETE query. Note : currently, only {@link com.datastax.driver.mapping.Mapper.Option.Timestamp}
     * is supported for DELETE queries.
     * <p/>
     * This method is useful if you want to setup a number of options (tracing,
     * conistency level, ...) of the returned statement before executing it manually
     * or need access to the {@code ResultSet} object after execution (to get the
     * trace, the execution info, ...), but in other cases, calling {@link #delete}
     * or {@link #deleteAsync} is shorter.
     * <p/>
     * This method allows you to provide a suite of {@link Option} to include in
     * the DELETE query. Options currently supported for DELETE are :
     * <ul>
     * <li>Timestamp</li>
     * <li>Consistency level</li>
     * <li>Tracing</li>
     * </ul>
     *
     * @param entity  the entity to delete.
     * @param options the options to add to the DELETE query.
     * @return a query that delete {@code entity} (based on it's defined mapping) with
     * provided USING options.
     */
    public Statement deleteQuery(T entity, Option... options) {
        try {
            return Uninterruptibles.getUninterruptibly(deleteQueryAsync(entity, toMapWithDefaults(options, defaultDeleteOptions)));
        } catch (ExecutionException e) {
            throw DriverThrowables.propagateCause(e);
        }
    }

    /**
     * Creates a query that can be used to delete the provided entity.
     * <p/>
     * This method is a shortcut that extract the PRIMARY KEY from the
     * provided entity and call {@link #deleteQuery(Object...)} with it.
     * <p/>
     * This method is useful if you want to setup a number of options (tracing,
     * conistency level, ...) of the returned statement before executing it manually
     * or need access to the {@code ResultSet} object after execution (to get the
     * trace, the execution info, ...), but in other cases, calling {@link #delete}
     * or {@link #deleteAsync} is shorter.
     *
     * @param entity the entity to delete.
     * @return a query that delete {@code entity} (based on it's defined mapping).
     */
    public Statement deleteQuery(T entity) {
        try {
            return Uninterruptibles.getUninterruptibly(deleteQueryAsync(entity, defaultDeleteOptions));
        } catch (ExecutionException e) {
            throw DriverThrowables.propagateCause(e);
        }
    }

    /**
     * Creates a query that can be used to delete an entity given its PRIMARY KEY.
     * <p/>
     * The values provided must correspond to the columns composing the PRIMARY
     * KEY (in the order of said primary key). The values can also contain, after
     * specifying the primary keys columns, a suite of {@link Option} to include in
     * the DELETE query. Note : currently, only {@link com.datastax.driver.mapping.Mapper.Option.Timestamp}
     * is supported for DELETE queries.
     * <p/>
     * This method is useful if you want to setup a number of options (tracing,
     * conistency level, ...) of the returned statement before executing it manually
     * or need access to the {@code ResultSet} object after execution (to get the
     * trace, the execution info, ...), but in other cases, calling {@link #delete}
     * or {@link #deleteAsync} is shorter.
     * This method allows you to provide a suite of {@link Option} to include in
     * the DELETE query. Options currently supported for DELETE are :
     * <ul>
     * <li>Timestamp</li>
     * <li>Consistency level</li>
     * <li>Tracing</li>
     * </ul>
     *
     * @param objects the primary key of the entity to delete, or more precisely
     *                the values for the columns of said primary key in the order of the primary key.
     *                Can be followed by {@link Option} to include in the DELETE
     *                query.
     * @return a query that delete the entity of PRIMARY KEY {@code primaryKey}.
     * @throws IllegalArgumentException if the number of value provided differ from
     *                                  the number of columns composing the PRIMARY KEY of the mapped class, or if
     *                                  at least one of those values is {@code null}.
     */
    public Statement deleteQuery(Object... objects) {
        try {
            return Uninterruptibles.getUninterruptibly(deleteQueryAsync(objects));
        } catch (ExecutionException e) {
            throw DriverThrowables.propagateCause(e);
        }
    }

    private ListenableFuture<BoundStatement> deleteQueryAsync(T entity, EnumMap<Option.Type, Option> options) {
        List<Object> pks = new ArrayList<Object>();
        for (int i = 0; i < mapper.primaryKeySize(); i++) {
            pks.add(mapper.getPrimaryKeyColumn(i).getValue(entity));
        }
        return deleteQueryAsync(pks, options);
    }

    private ListenableFuture<BoundStatement> deleteQueryAsync(Object... objects) {
        // Order and duplicates matter for primary keys
        List<Object> pks = new ArrayList<Object>();
        EnumMap<Option.Type, Option> options = new EnumMap<Option.Type, Option>(defaultDeleteOptions);
        for (Object o : objects) {
            if (o instanceof Option) {
                Option option = (Option) o;
                options.put(option.type, option);
            } else {
                pks.add(o);
            }
        }
        return deleteQueryAsync(pks, options);
    }

    private ListenableFuture<BoundStatement> deleteQueryAsync(final List<Object> primaryKey, final EnumMap<Option.Type, Option> options) {
        if (primaryKey.size() != mapper.primaryKeySize())
            throw new IllegalArgumentException(String.format("Invalid number of PRIMARY KEY columns provided, %d expected but got %d", mapper.primaryKeySize(), primaryKey.size()));

        return Futures.transform(getPreparedQueryAsync(QueryType.DEL, options), new Function<PreparedStatement, BoundStatement>() {
            @Override
            public BoundStatement apply(PreparedStatement input) {
                BoundStatement bs = input.bind();
                if (mapper.writeConsistency != null)
                    bs.setConsistencyLevel(mapper.writeConsistency);

                int i = 0;
                for (Option opt : options.values()) {
                    opt.checkValidFor(QueryType.DEL, manager);
                    opt.addToPreparedStatement(bs, i);
                    if (opt.isIncludedInQuery())
                        i++;
                }

                int columnNumber = 0;
                for (Object value : primaryKey) {
                    ColumnMapper<T> column = mapper.getPrimaryKeyColumn(columnNumber);
                    if (value == null) {
                        throw new IllegalArgumentException(String.format("Invalid null value for PRIMARY KEY column %s (argument %d)", column.getColumnName(), i));
                    }
                    setObject(bs, i++, value, column);
                    columnNumber++;
                }
                return bs;
            }
        });
    }

    /**
     * Deletes an entity mapped by this mapper.
     * <p/>
     * This method is basically equivalent to: {@code getManager().getSession().execute(deleteQuery(entity))}.
     *
     * @param entity the entity to delete.
     */
    public void delete(T entity) {
        try {
            Uninterruptibles.getUninterruptibly(deleteAsync(entity));
        } catch (ExecutionException e) {
            throw DriverThrowables.propagateCause(e);
        }
    }

    /**
     * Deletes an entity mapped by this mapper using provided options.
     * <p/>
     * This method is basically equivalent to: {@code getManager().getSession().execute(deleteQuery(entity, options))}.
     *
     * @param entity  the entity to delete.
     * @param options the options to add to the DELETE query.
     */
    public void delete(T entity, Option... options) {
        try {
            Uninterruptibles.getUninterruptibly(deleteAsync(entity, options));
        } catch (ExecutionException e) {
            throw DriverThrowables.propagateCause(e);
        }
    }

    /**
     * Deletes an entity mapped by this mapper asynchronously.
     * <p/>
     * This method is basically equivalent to: {@code getManager().getSession().executeAsync(deleteQuery(entity))}.
     *
     * @param entity the entity to delete.
     * @return a future on the completion of the deletion.
     */
    public ListenableFuture<Void> deleteAsync(T entity) {
        return submitVoidQueryAsync(deleteQueryAsync(entity, defaultDeleteOptions));
    }

    /**
     * Deletes an entity mapped by this mapper asynchronously using provided options.
     * <p/>
     * This method is basically equivalent to: {@code getManager().getSession().executeAsync(deleteQuery(entity, options))}.
     *
     * @param entity  the entity to delete.
     * @param options the options to add to the DELETE query.
     * @return a future on the completion of the deletion.
     */
    public ListenableFuture<Void> deleteAsync(T entity, Option... options) {
        return submitVoidQueryAsync(deleteQueryAsync(entity, toMapWithDefaults(options, defaultDeleteOptions)));
    }

    /**
     * Deletes an entity based on its primary key.
     * <p/>
     * This method is basically equivalent to: {@code getManager().getSession().execute(deleteQuery(objects))}.
     *
     * @param objects the primary key of the entity to delete, or more precisely
     *                the values for the columns of said primary key in the order
     *                of the primary key.Can be followed by {@link Option} to include
     *                in the DELETE query.
     * @throws IllegalArgumentException if the number of value provided differ from
     *                                  the number of columns composing the PRIMARY KEY of the mapped class, or if
     *                                  at least one of those values is {@code null}.
     */
    public void delete(Object... objects) {
        try {
            Uninterruptibles.getUninterruptibly(deleteAsync(objects));
        } catch (ExecutionException e) {
            throw DriverThrowables.propagateCause(e);
        }
    }

    /**
     * Deletes an entity based on its primary key asynchronously.
     * <p/>
     * This method is basically equivalent to: {@code getManager().getSession().executeAsync(deleteQuery(objects))}.
     *
     * @param objects the primary key of the entity to delete, or more precisely
     *                the values for the columns of said primary key in the order
     *                of the primary key. Can be followed by {@link Option} to include
     *                in the DELETE query.
     * @throws IllegalArgumentException if the number of value provided differ from
     *                                  the number of columns composing the PRIMARY KEY of the mapped class, or if
     *                                  at least one of those values is {@code null}.
     */
    public ListenableFuture<Void> deleteAsync(Object... objects) {
        return submitVoidQueryAsync(deleteQueryAsync(objects));
    }

    /**
     * Maps the rows from a {@code ResultSet} into the class this is a mapper of.
     * <p/>
     * If the query was user-generated (that is, passed directly to {@code session.execute()} or configured with
     * {@link com.datastax.driver.mapping.annotations.Query @Query} in an
     * {@link com.datastax.driver.mapping.annotations.Accessor @Accessor}-annotated interface), then only the columns
     * present in the result set will be mapped to the corresponding fields, and {@link Computed} fields will not be
     * populated.
     * <p/>
     * If the query was generated by the mapper (for example with {@link #getQuery(Object...)}), all fields will be
     * mapped, as if the object came from a direct {@link #get(Object...)} call.
     *
     * @param resultSet the {@code ResultSet} to map.
     * @return the mapped result set. Note that the returned mapped result set
     * will encapsulate {@code resultSet} and so consuming results from this
     * returned mapped result set will consume results from {@code resultSet}
     * and vice-versa.
     */
    public Result<T> map(ResultSet resultSet) {
        boolean useAlias = !manager.isCassandraV1 && isFromMapperQuery(resultSet);
        return new Result<T>(resultSet, mapper, useAlias);
    }

    /**
     * Asynchronously maps the rows from a {@link ResultSetFuture} into the class this is a mapper of.
     * <p/>
     * Use this method to map a {@link ResultSetFuture} that was not generated by the mapper
     * (e.g. a {@link ResultSetFuture} coming from a manual query or an {@link Accessor} method).
     * It expects that the result set contains all column mapped in the target class,
     * and that they are not aliased. {@link Computed} fields will not be filled in mapped objects.
     *
     * @param resultSetFuture the {@link ResultSetFuture} to map.
     * @return the mapped result set future. Note that the returned mapped result set
     * will encapsulate the result set returned by {@code resultSetFuture} and so consuming results from this
     * returned mapped result set will consume results from that result set
     * and vice-versa.
     */
    public ListenableFuture<Result<T>> mapAsync(ResultSetFuture resultSetFuture) {
        return Futures.transform(resultSetFuture, new Function<ResultSet, Result<T>>() {
            @Override
            public Result<T> apply(ResultSet rs) {
                return map(rs);
            }
        });
    }

    private boolean isFromMapperQuery(ResultSet resultSet) {
        return resultSet.getExecutionInfo().getStatement() instanceof MapperBoundStatement;
    }

    /**
     * @deprecated you no longer need to specify whether a result set is aliased, it will be detected automatically. Use
     * {@link #map(ResultSet)} instead of this method.
     */
    @Deprecated
    public Result<T> mapAliased(ResultSet resultSet) {
        return (manager.isCassandraV1)
                ? map(resultSet) // no aliases
                : new Result<T>(resultSet, mapper, true);
    }

    /**
     * Set the default save {@link Option} for this object mapper, that will be used
     * in all save operations unless overridden. Refer to {@link Mapper#save(Object, Option...)})}
     * to check available save options.
     *
     * @param options the options to set. To reset, use {@link Mapper#resetDefaultSaveOptions}.
     */
    public void setDefaultSaveOptions(Option... options) {
        this.defaultSaveOptions = toMap(options);
    }

    /**
     * Reset the default save options for this object mapper.
     */
    public void resetDefaultSaveOptions() {
        this.defaultSaveOptions = NO_OPTIONS;
    }

    /**
     * Set the default get {@link Option} for this object mapper, that will be used
     * in all get operations unless overridden. Refer to {@link Mapper#get(Object...)} )} to check available
     * get options.
     *
     * @param options the options to set. To reset, use {@link Mapper#resetDefaultGetOptions}.
     */
    public void setDefaultGetOptions(Option... options) {
        this.defaultGetOptions = toMap(options);

    }

    /**
     * Reset the default save options for this object mapper.
     */
    public void resetDefaultGetOptions() {
        this.defaultGetOptions = NO_OPTIONS;
    }

    /**
     * Set the default delete {@link Option} for this object mapper, that will be used
     * in all delete operations unless overridden. Refer to {@link Mapper#delete(Object...)} )}
     * to check available delete options.
     *
     * @param options the options to set. To reset, use {@link Mapper#resetDefaultDeleteOptions}.
     */
    public void setDefaultDeleteOptions(Option... options) {
        this.defaultDeleteOptions = toMap(options);
    }

    /**
     * Reset the default delete options for this object mapper.
     */
    public void resetDefaultDeleteOptions() {
        this.defaultDeleteOptions = NO_OPTIONS;
    }

    private static EnumMap<Option.Type, Option> toMap(Option[] options) {
        EnumMap<Option.Type, Option> result = new EnumMap<Option.Type, Option>(Option.Type.class);
        for (Option option : options) {
            result.put(option.type, option);
        }
        return result;
    }

    private static EnumMap<Option.Type, Option> toMapWithDefaults(Option[] options, EnumMap<Option.Type, Option> defaults) {
        EnumMap<Option.Type, Option> result = new EnumMap<Option.Type, Option>(defaults);
        for (Option option : options) {
            result.put(option.type, option);
        }
        return result;
    }

    /**
     * An option for a mapper operation.
     * <p/>
     * Options can be passed to individual operations:
     * <pre>
     *     mapper.save(myObject, Option.ttl(3600));
     * </pre>
     * <p/>
     * The mapper can also have defaults, that will apply to all operations that do not
     * override these particular option:
     * <pre>
     *     mapper.setDefaultSaveOptions(Option.ttl(3600));
     *     mapper.save(myObject);
     * </pre>
     * <p/>
     * <p/>
     * See the static methods in this class for available options.
     */
    public static abstract class Option {

        enum Type {TTL, TIMESTAMP, CL, TRACING, SAVE_NULL_FIELDS}

        final Type type;

        protected Option(Type type) {
            this.type = type;
        }

        /**
         * Creates a new Option object to add time-to-live to a mapper operation. This is
         * only valid for save operations.
         * <p/>
         * Note that this option is only available if using {@link ProtocolVersion#V2} or above.
         *
         * @param ttl the TTL (in seconds).
         * @return the option.
         */
        public static Option ttl(int ttl) {
            return new Ttl(ttl);
        }

        /**
         * Creates a new Option object to add a timestamp to a mapper operation. This is
         * only valid for save and delete operations.
         * <p/>
         * Note that this option is only available if using {@link ProtocolVersion#V2} or above.
         *
         * @param timestamp the timestamp (in microseconds).
         * @return the option.
         */
        public static Option timestamp(long timestamp) {
            return new Timestamp(timestamp);
        }

        /**
         * Creates a new Option object to add a consistency level value to a mapper operation. This
         * is valid for save, delete and get operations.
         * <p/>
         * Note that the consistency level can also be defined at the mapper level, as a parameter
         * of the {@link com.datastax.driver.mapping.annotations.Table} annotation (this is redundant
         * for backward compatibility). This option, whether defined on a specific call or as the
         * default, will always take precedence over the annotation.
         *
         * @param cl the {@link com.datastax.driver.core.ConsistencyLevel} to use for the operation.
         * @return the option.
         */
        public static Option consistencyLevel(ConsistencyLevel cl) {
            return new ConsistencyLevelOption(cl);
        }

        /**
         * Creates a new Option object to enable query tracing for a mapper operation. This
         * is valid for save, delete and get operations.
         *
         * @param enabled whether to enable tracing.
         * @return the option.
         */
        public static Option tracing(boolean enabled) {
            return new Tracing(enabled);
        }

        /**
         * Creates a new Option object to specify whether null entity fields should be included in
         * insert queries. This option is valid only for save operations.
         * <p/>
         * If this option is not specified, it defaults to {@code true} (null fields are saved).
         *
         * @param enabled whether to include null fields in queries.
         * @return the option.
         */
        public static Option saveNullFields(boolean enabled) {
            return new SaveNullFields(enabled);
        }

        public Type getType() {
            return this.type;
        }

        abstract void appendTo(Insert.Options usings);

        abstract void appendTo(Delete.Options usings);

        abstract void addToPreparedStatement(BoundStatement bs, int i);

        abstract void checkValidFor(QueryType qt, MappingManager manager) throws IllegalArgumentException;

        abstract boolean isIncludedInQuery();

        static class Ttl extends Option {

            private final int ttlValue;

            Ttl(int value) {
                super(Type.TTL);
                this.ttlValue = value;
            }

            @Override
            void appendTo(Insert.Options usings) {
                usings.and(QueryBuilder.ttl(QueryBuilder.bindMarker()));
            }

            @Override
            void appendTo(Delete.Options usings) {
                throw new UnsupportedOperationException("shouldn't be called");
            }

            @Override
            void addToPreparedStatement(BoundStatement bs, int i) {
                bs.setInt(i, this.ttlValue);
            }

            @Override
            void checkValidFor(QueryType qt, MappingManager manager) {
                checkArgument(!manager.isCassandraV1, "TTL option requires native protocol v2 or above");
                checkArgument(qt == QueryType.SAVE, "TTL option is only allowed in save queries");
            }

            @Override
            boolean isIncludedInQuery() {
                return true;
            }
        }

        static class Timestamp extends Option {

            private final long tsValue;

            Timestamp(long value) {
                super(Type.TIMESTAMP);
                this.tsValue = value;
            }

            @Override
            void appendTo(Insert.Options usings) {
                usings.and(QueryBuilder.timestamp(QueryBuilder.bindMarker()));
            }

            @Override
            void appendTo(Delete.Options usings) {
                usings.and(QueryBuilder.timestamp(QueryBuilder.bindMarker()));
            }

            @Override
            void checkValidFor(QueryType qt, MappingManager manager) {
                checkArgument(!manager.isCassandraV1, "Timestamp option requires native protocol v2 or above");
                checkArgument(qt == QueryType.SAVE || qt == QueryType.DEL, "Timestamp option is only allowed in save and delete queries");
            }

            @Override
            void addToPreparedStatement(BoundStatement bs, int i) {
                bs.setLong(i, this.tsValue);
            }

            @Override
            boolean isIncludedInQuery() {
                return true;
            }
        }

        static class ConsistencyLevelOption extends Option {

            private final ConsistencyLevel cl;

            ConsistencyLevelOption(ConsistencyLevel cl) {
                super(Type.CL);
                this.cl = cl;
            }

            @Override
            void appendTo(Insert.Options usings) {
                throw new UnsupportedOperationException("shouldn't be called");
            }

            @Override
            void appendTo(Delete.Options usings) {
                throw new UnsupportedOperationException("shouldn't be called");
            }

            @Override
            void addToPreparedStatement(BoundStatement bs, int i) {
                bs.setConsistencyLevel(cl);
            }

            @Override
            void checkValidFor(QueryType qt, MappingManager manager) {
                checkArgument(qt == QueryType.SAVE || qt == QueryType.DEL || qt == QueryType.GET,
                        "Consistency level option is only allowed in save, delete and get queries");
            }

            @Override
            boolean isIncludedInQuery() {
                return false;
            }
        }

        static class Tracing extends Option {

            private final boolean tracing;

            Tracing(boolean tracing) {
                super(Type.TRACING);
                this.tracing = tracing;
            }

            @Override
            void appendTo(Insert.Options usings) {
                throw new UnsupportedOperationException("shouldn't be called");
            }

            @Override
            void appendTo(Delete.Options usings) {
                throw new UnsupportedOperationException("shouldn't be called");
            }

            @Override
            void addToPreparedStatement(BoundStatement bs, int i) {
                if (this.tracing)
                    bs.enableTracing();
            }

            @Override
            void checkValidFor(QueryType qt, MappingManager manager) {
                checkArgument(qt == QueryType.SAVE || qt == QueryType.DEL || qt == QueryType.GET,
                        "Tracing option is only allowed in save, delete and get queries");
            }

            @Override
            boolean isIncludedInQuery() {
                return false;
            }
        }

        static class SaveNullFields extends Option {

            private final boolean saveNullFields;

            SaveNullFields(boolean saveNullFields) {
                super(SAVE_NULL_FIELDS);
                this.saveNullFields = saveNullFields;
            }

            @Override
            void appendTo(Insert.Options usings) {
                throw new UnsupportedOperationException("shouldn't be called");
            }

            @Override
            void appendTo(Delete.Options usings) {
                throw new UnsupportedOperationException("shouldn't be called");
            }

            @Override
            void addToPreparedStatement(BoundStatement bs, int i) {
                // nothing to do
            }

            @Override
            void checkValidFor(QueryType qt, MappingManager manager) {
                checkArgument(qt == QueryType.SAVE, "SaveNullFields option is only allowed in save queries");
            }

            @Override
            boolean isIncludedInQuery() {
                return false;
            }
        }

    }

    private static class MapperQueryKey {
        private final QueryType queryType;
        private final EnumSet<Option.Type> optionTypes;
        private final Set<ColumnMapper<?>> columns;

        MapperQueryKey(QueryType queryType, Set<ColumnMapper<?>> columnMappers, EnumMap<Option.Type, Option> options) {
            Preconditions.checkNotNull(queryType);
            Preconditions.checkNotNull(options);
            Preconditions.checkNotNull(columnMappers);
            this.queryType = queryType;
            this.columns = columnMappers;
            this.optionTypes = EnumSet.noneOf(Option.Type.class);
            for (Option opt : options.values()) {
                if (opt.isIncludedInQuery())
                    this.optionTypes.add(opt.type);
            }
        }

        @Override
        public boolean equals(Object other) {
            if (this == other)
                return true;
            if (other instanceof MapperQueryKey) {
                MapperQueryKey that = (MapperQueryKey) other;
                return this.queryType.equals(that.queryType)
                        && this.optionTypes.equals(that.optionTypes)
                        && this.columns.equals(that.columns);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(queryType, optionTypes, columns);
        }
    }
}
