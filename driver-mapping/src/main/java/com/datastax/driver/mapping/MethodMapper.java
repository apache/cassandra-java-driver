/*
 *      Copyright (C) 2012-2014 DataStax Inc.
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

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.datastax.driver.core.*;

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

        if (method.isVarArgs())
            throw new IllegalArgumentException(String.format("Invalid varargs method %s in @Accessor interface"));
        if (ps.getVariables().size() != method.getParameterTypes().length)
            throw new IllegalArgumentException(String.format("The number of arguments for method %s (%d) does not match the number of bind parameters in the @Query (%d)",
                                                              method.getName(), method.getParameterTypes().length, ps.getVariables().size()));

        // TODO: we should also validate the types of the parameters...

        Class<?> returnType = method.getReturnType();
        if (Void.class.isAssignableFrom(returnType) || ResultSet.class.isAssignableFrom(returnType))
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
            Type k = ((ParameterizedType)method.getGenericReturnType()).getActualTypeArguments()[0];
            if (k instanceof Class && ResultSet.class.isAssignableFrom((Class<?>)k))
                return;

            mapType(manager, returnType, k);
        } else {
            mapType(manager, returnType, method.getGenericReturnType());
        }
    }

    @SuppressWarnings("rawtypes")
	private void mapType(MappingManager manager, Class<?> fullReturnType, Type type) {

        if (type instanceof ParameterizedType) {
            ParameterizedType pt = (ParameterizedType)type;
            Type raw = pt.getRawType();
            if (raw instanceof Class && Result.class.isAssignableFrom((Class)raw)) {
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
            this.returnMapper = (Mapper<?>)manager.mapper((Class<?>)type);
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
                 ? Futures.transform(future, returnMapper.mapOneFunction)
                 : Futures.transform(future, returnMapper.mapAllFunction);
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

        public ParamMapper(String paramName, int paramIdx) {
            this.paramName = paramName;
            this.paramIdx = paramIdx;
        }

        void setValue(BoundStatement boundStatement, Object arg, ProtocolVersion protocolVersion) {
				if (paramName == null) {
               if (arg == null) boundStatement.setToNull(paramIdx); else boundStatement.setBytesUnsafe(paramIdx, DataType.serializeValue(arg, protocolVersion));
           } else {
          	 if (arg == null) boundStatement.setToNull(paramName); else boundStatement.setBytesUnsafe(paramName, DataType.serializeValue(arg, protocolVersion));
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
            super.setValue(boundStatement, udtMapper.toUDT(entity), protocolVersion);
        }
    }

    static class UDTListParamMapper<V> extends ParamMapper {
        private final UDTMapper<V> valueMapper;

        UDTListParamMapper(String paramName, int paramIdx, UDTMapper<V> valueMapper) {
            super(paramName, paramIdx);
            this.valueMapper = valueMapper;
        }

        @Override
        void setValue(BoundStatement boundStatement, Object arg, ProtocolVersion protocolVersion) {
            @SuppressWarnings("unchecked")
            List<V> entities = (List<V>) arg;
            super.setValue(boundStatement, valueMapper.toUDTValues(entities), protocolVersion);
        }
    }

    static class UDTSetParamMapper<V> extends ParamMapper {
        private final UDTMapper<V> valueMapper;

        UDTSetParamMapper(String paramName, int paramIdx, UDTMapper<V> valueMapper) {
            super(paramName, paramIdx);
            this.valueMapper = valueMapper;
        }

        @Override
        void setValue(BoundStatement boundStatement, Object arg, ProtocolVersion protocolVersion) {
            @SuppressWarnings("unchecked")
            Set<V> entities = (Set<V>) arg;
            super.setValue(boundStatement, valueMapper.toUDTValues(entities), protocolVersion);
        }
    }

    static class UDTMapParamMapper<K, V> extends ParamMapper {
        private final UDTMapper<K> keyMapper;
        private final UDTMapper<V> valueMapper;

        UDTMapParamMapper(String paramName, int paramIdx, UDTMapper<K> keyMapper, UDTMapper<V> valueMapper) {
            super(paramName, paramIdx);
            this.keyMapper = keyMapper;
            this.valueMapper = valueMapper;
        }

        @Override
        void setValue(BoundStatement boundStatement, Object arg, ProtocolVersion protocolVersion) {
            @SuppressWarnings("unchecked")
            Map<K, V> entities = (Map<K, V>) arg;
            super.setValue(boundStatement, UDTMapper.toUDTValues(entities, keyMapper, valueMapper), protocolVersion);
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
