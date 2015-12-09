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
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.Set;

// TODO: we probably should make that an abstract class and move some bit in a "ReflexionMethodMapper"
// subclass for consistency with the rest, but we can see to that later
class MethodMapper {

    public final Method method;
    public final String queryString;
    public final ParamMapper[] paramMappers;

    private final ConsistencyLevel consistency;
    private final int fetchSize;
    private final boolean tracing;

    private Session session;
    private PreparedStatement statement;

    private boolean returnStatement;
    private Mapper<?> returnMapper;
    private boolean mapOne;
    private boolean async;

    MethodMapper(Method method, String queryString, ParamMapper[] paramMappers, ConsistencyLevel consistency, int fetchSize, boolean enableTracing) {
        this.method = method;
        this.queryString = queryString;
        this.paramMappers = paramMappers;
        this.consistency = consistency;
        this.fetchSize = fetchSize;
        this.tracing = enableTracing;
    }

    public void prepare(MappingManager manager, PreparedStatement ps) {
        this.session = manager.getSession();
        this.statement = ps;

        validateParameters();

        Class<?> returnType = method.getReturnType();
        if (Void.TYPE.isAssignableFrom(returnType) || ResultSet.class.isAssignableFrom(returnType))
            return;

        if (Statement.class.isAssignableFrom(returnType)) {
            returnStatement = true;
            return;
        }

        if (ResultSetFuture.class.isAssignableFrom(returnType)) {
            this.async = true;
            return;
        }

        if (ListenableFuture.class.isAssignableFrom(returnType)) {
            this.async = true;
            Type k = ((ParameterizedType) method.getGenericReturnType()).getActualTypeArguments()[0];
            if (k instanceof Class && ResultSet.class.isAssignableFrom((Class<?>) k))
                return;

            mapType(manager, returnType, k);
        } else {
            mapType(manager, returnType, method.getGenericReturnType());
        }
    }

    // Checks the method parameters against the query's bind variables
    private void validateParameters() {
        if (method.isVarArgs())
            throw new IllegalArgumentException(String.format("Invalid varargs method %s in @Accessor interface", method.getName()));

        ColumnDefinitions variables = statement.getVariables();
        Set<String> names = Sets.newHashSet();
        for (ColumnDefinitions.Definition variable : variables) {
            names.add(variable.getName());
        }

        if (method.getParameterTypes().length < names.size())
            throw new IllegalArgumentException(String.format("Not enough arguments for method %s, "
                            + "found %d but it should be at least the number of unique bind parameter names in the @Query (%d)",
                    method.getName(), method.getParameterTypes().length, names.size()));

        if (method.getParameterTypes().length > variables.size())
            throw new IllegalArgumentException(String.format("Too many arguments for method %s, "
                            + "found %d but it should be at most the number of bind parameters in the @Query (%d)",
                    method.getName(), method.getParameterTypes().length, variables.size()));

        // TODO could go further, e.g. check that the types match, inspect @Param annotations to check that all names are bound...
    }

    @SuppressWarnings("rawtypes")
    private void mapType(MappingManager manager, Class<?> fullReturnType, Type type) {

        if (type instanceof ParameterizedType) {
            ParameterizedType pt = (ParameterizedType) type;
            Type raw = pt.getRawType();
            if (raw instanceof Class && Result.class.isAssignableFrom((Class) raw)) {
                type = pt.getActualTypeArguments()[0];
            } else {
                mapOne = true;
            }
        } else {
            mapOne = true;
        }

        if (!(type instanceof Class))
            throw new RuntimeException(String.format("Cannot map return of method %s to unsupported type %s", method, type));

        try {
            this.returnMapper = (Mapper<?>) manager.mapper((Class<?>) type);
        } catch (Exception e) {
            throw new RuntimeException("Cannot map return to class " + fullReturnType, e);
        }
    }

    public Object invoke(Object[] args) {

        BoundStatement bs = statement.bind();

        ProtocolVersion protocolVersion = session.getCluster().getConfiguration().getProtocolOptions().getProtocolVersionEnum();
        for (int i = 0; i < args.length; i++) {
            paramMappers[i].setValue(bs, args[i], protocolVersion);
        }

        if (consistency != null)
            bs.setConsistencyLevel(consistency);
        if (fetchSize > 0)
            bs.setFetchSize(fetchSize);
        if (tracing)
            bs.enableTracing();

        if (returnStatement)
            return bs;

        if (async) {
            ListenableFuture<ResultSet> future = session.executeAsync(bs);
            if (returnMapper == null)
                return future;

            return mapOne
                    ? Futures.transform(future, returnMapper.mapOneFunctionWithoutAliases)
                    : Futures.transform(future, returnMapper.mapAllFunctionWithoutAliases);
        } else {
            ResultSet rs = session.execute(bs);
            if (returnMapper == null)
                return rs;

            Result<?> result = returnMapper.map(rs);
            return mapOne ? result.one() : result;
        }
    }

    static class ParamMapper {
        // We'll only set one of the other. If paramName is null, then paramIdx is used.
        private final String paramName;
        private final int paramIdx;
        private final DataType dataType;

        public ParamMapper(String paramName, int paramIdx, DataType dataType) {
            this.paramName = paramName;
            this.paramIdx = paramIdx;
            this.dataType = dataType;
        }

        public ParamMapper(String paramName, int paramIdx) {
            this(paramName, paramIdx, null);
        }

        void setValue(BoundStatement boundStatement, Object arg, ProtocolVersion protocolVersion) {
            ByteBuffer serializedArg = (dataType == null)
                    ? DataType.serializeValue(arg, protocolVersion)
                    : dataType.serialize(arg, protocolVersion);
            if (paramName == null) {
                if (arg == null)
                    boundStatement.setToNull(paramIdx);
                else
                    boundStatement.setBytesUnsafe(paramIdx, serializedArg);
            } else {
                if (arg == null)
                    boundStatement.setToNull(paramName);
                else
                    boundStatement.setBytesUnsafe(paramName, serializedArg);
            }
        }
    }

    static class UDTParamMapper<V> extends ParamMapper {
        private final UDTMapper<V> udtMapper;

        UDTParamMapper(String paramName, int paramIdx, UDTMapper<V> udtMapper) {
            super(paramName, paramIdx);
            this.udtMapper = udtMapper;
        }

        @Override
        void setValue(BoundStatement boundStatement, Object arg, ProtocolVersion protocolVersion) {
            @SuppressWarnings("unchecked")
            V entity = (V) arg;
            UDTValue udtArg = arg != null ? udtMapper.toUDT(entity) : null;
            super.setValue(boundStatement, udtArg, protocolVersion);
        }
    }

    /**
     * Maps a nested collection which has a mapped UDT somewhere in the hierarchy.
     */
    static class NestedUDTParamMapper extends ParamMapper {
        private final InferredCQLType inferredCQLType;

        NestedUDTParamMapper(String paramName, int paramIdx, InferredCQLType inferredCQLType) {
            super(paramName, paramIdx);
            this.inferredCQLType = inferredCQLType;
        }

        @Override
        void setValue(BoundStatement boundStatement, Object arg, ProtocolVersion protocolVersion) {
            super.setValue(boundStatement,
                    UDTMapper.convertEntitiesToUDTs(arg, inferredCQLType),
                    protocolVersion);
        }
    }

    static class EnumParamMapper extends ParamMapper {

        private final EnumType enumType;

        public EnumParamMapper(String paramName, int paramIdx, EnumType enumType) {
            super(paramName, paramIdx);
            this.enumType = enumType;
        }

        @Override
        void setValue(BoundStatement boundStatement, Object arg, ProtocolVersion protocolVersion) {
            super.setValue(boundStatement, convert(arg), protocolVersion);
        }

        @SuppressWarnings("rawtypes")
        private Object convert(Object arg) {
            if (arg == null)
                return arg;

            switch (enumType) {
                case STRING:
                    return arg.toString();
                case ORDINAL:
                    return ((Enum) arg).ordinal();
            }
            throw new AssertionError();
        }
    }
}
