package com.datastax.driver.mapping;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

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

        for (int i = 0; i < args.length; i++) {
            paramMappers[i].setValue(bs, args[i]);
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
        private final String paramName;

        public ParamMapper(String paramName) {
            this.paramName = paramName;
        }

        void setValue(BoundStatement boundStatement, Object arg) {
            if (arg != null) {
                boundStatement.setBytesUnsafe(paramName, DataType.serializeValue(arg));
            }
        }
    }
    
    static class UDTParamMapper<V> extends ParamMapper {
        private final NestedMapper<V> nestedMapper;

        UDTParamMapper(String paramName, NestedMapper<V> nestedMapper) {
            super(paramName);
            this.nestedMapper = nestedMapper;
        }
        
        @Override
        void setValue(BoundStatement boundStatement, Object arg) {
            @SuppressWarnings("unchecked")
            V entity = (V) arg;
            super.setValue(boundStatement, nestedMapper.toUDTValue(entity));
        }
    }
    
    static class UDTListParamMapper<V> extends ParamMapper {
        private final NestedMapper<V> valueMapper;

        UDTListParamMapper(String paramName, NestedMapper<V> valueMapper) {
            super(paramName);
            this.valueMapper = valueMapper;
        }
        
        @Override
        void setValue(BoundStatement boundStatement, Object arg) {
            @SuppressWarnings("unchecked")
            List<V> nestedEntities = (List<V>) arg;
            super.setValue(boundStatement, valueMapper.toUDTValues(nestedEntities));
        }
    }
    
    static class UDTSetParamMapper<V> extends ParamMapper {
        private final NestedMapper<V> valueMapper;
        
        UDTSetParamMapper(String paramName, NestedMapper<V> valueMapper) {
            super(paramName);
            this.valueMapper = valueMapper;
        }
        
        @Override
        void setValue(BoundStatement boundStatement, Object arg) {
            @SuppressWarnings("unchecked")
            Set<V> nestedEntities = (Set<V>) arg;
            super.setValue(boundStatement, valueMapper.toUDTValues(nestedEntities));
        }
    }
    
    static class UDTMapParamMapper<K, V> extends ParamMapper {
        private final NestedMapper<K> keyMapper;
        private final NestedMapper<V> valueMapper;
        
        UDTMapParamMapper(String paramName, NestedMapper<K> keyMapper, NestedMapper<V> valueMapper) {
            super(paramName);
            this.keyMapper = keyMapper;
            this.valueMapper = valueMapper;
        }
        
        @Override
        void setValue(BoundStatement boundStatement, Object arg) {
            @SuppressWarnings("unchecked")
            Map<K, V> nestedEntities = (Map<K, V>) arg;
            super.setValue(boundStatement, NestedMapper.toUDTValues(nestedEntities, keyMapper, valueMapper));
        }
    }
}
