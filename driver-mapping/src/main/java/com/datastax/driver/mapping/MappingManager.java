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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Table;
import com.datastax.driver.mapping.annotations.UDT;

/**
 * Mapping manager from which to obtain entity mappers.
 */
public class MappingManager {

    private final Session session;
    final boolean isCassandraV1;

    private volatile Map<Class<?>, Mapper<?>> mappers = Collections.emptyMap();
    private volatile Map<Class<?>, MappedUDTCodec<?>> udtCodecs = Collections.emptyMap();
    private volatile Map<Class<?>, Object> accessors = Collections.emptyMap();

    /**
     * Creates a new {@code MappingManager} using the provided {@code Session}.
     * <p>
     * Note that this constructor forces the initialization of the session (see
     * {@link #MappingManager(Session, ProtocolVersion)} if that is a problem for you).
     *
     * @param session the {@code Session} to use.
     */
    public MappingManager(Session session) {
        this(session, getProtocolVersion(session));
    }

    private static ProtocolVersion getProtocolVersion(Session session) {
        session.init();
        return session.getCluster().getConfiguration().getProtocolOptions().getProtocolVersion();
    }

    /**
     * Creates a new {@code MappingManager} using the provided {@code Session}.
     * <p>
     * This constructor is only provided for backward compatibility: before 2.1.7, {@code MappingManager} could be
     * built from an uninitialized session; since 2.1.7, the mapper needs to know the active protocol version to
     * adapt its internal requests, so {@link #MappingManager(Session)} will now initialize the session if needed.
     * If you rely on the session not being initialized, use this constructor and provide the version manually.
     *
     * @param session the {@code Session} to use.
     * @param protocolVersion the protocol version that will be used with this session.
     *
     * @since 2.1.7
     */
    public MappingManager(Session session, ProtocolVersion protocolVersion) {
        this.session = session;
        // This is not strictly correct because we could connect to C* 2.0 with the v1 protocol.
        // But mappers need to make a decision early so that generated queries are compatible, and we don't know in advance
        // which nodes might join the cluster later.
        // At least if protocol >=2 we know there won't be any 1.2 nodes ever.
        this.isCassandraV1 = (protocolVersion == ProtocolVersion.V1);
    }

    /**
     * The underlying {@code Session} used by this manager.
     * <p>
     * Note that you can get obtain the {@code Cluster} object corresponding
     * to that session using {@code getSession().getCluster()}.
     * <p>
     * It is inadvisable to close the returned Session while this manager and
     * its mappers are in use.
     *
     * @return the underlying session used by this manager.
     */
    public Session getSession() {
        return session;
    }

    /**
     * Creates a {@code Mapper} for the provided class (that must be annotated by a
     * {@link Table} annotation).
     * <p>
     * The {@code MappingManager} only ever keeps one Mapper for each class, and so calling this
     * method multiple times on the same class will always return the same object.
     * <p>
     * If the type of any field in the class is an {@link UDT}-annotated classes, a codec for that
     * class will automatically be created and registered with the underlying {@code Cluster}.
     * This works recursively with UDTs nested in other UDTs or in collections.
     *
     * @param <T> the type of the class to map.
     * @param klass the (annotated) class for which to return the mapper.
     * @return the {@code Mapper} object for class {@code klass}.
     */
    public <T> Mapper<T> mapper(Class<T> klass) {
        return getMapper(klass);
    }

    /**
     * Creates a {@code TypeCodec} for the provided class (that must be annotated by
     * a {@link UDT} annotation).
     * <p>
     * This method also registers the codec against the underlying {@code Cluster}.
     * In addition, the codecs for any nested UDTs will also be created and registered.
     * <p>
     * You don't need to call this method explicitly if you already call {@link #mapper(Class)}
     * for a class that references this UDT class (creating a mapper will automatically
     * process all UDTs that it uses).
     *
     * @param <T> the type of the class to map.
     * @param klass the (annotated) class for which to return the codec.
     * @return the codec that maps the provided class to the corresponding user-defined type.
     */
    public <T> TypeCodec<T> udtCodec(Class<T> klass) {
        return getUDTCodec(klass);
    }

    /**
     * Creates an accessor object based on the provided interface (that must be annotated by
     * a {@link Accessor} annotation).
     * <p>
     * The {@code MappingManager} only ever keep one Accessor for each class, and so calling this
     * method multiple time on the same class will always return the same object.
     *
     * @param <T> the type of the accessor class.
     * @param klass the (annotated) class for which to create an accessor object.
     * @return the accessor object for class {@code klass}.
     */
    public <T> T createAccessor(Class<T> klass) {
        return getAccessor(klass);
    }

    @SuppressWarnings("unchecked")
    private <T> Mapper<T> getMapper(Class<T> klass) {
        Mapper<T> mapper = (Mapper<T>)mappers.get(klass);
        if (mapper == null) {
            synchronized (mappers) {
                mapper = (Mapper<T>)mappers.get(klass);
                if (mapper == null) {
                    EntityMapper<T> entityMapper = AnnotationParser.parseEntity(klass, ReflectionMapper.factory(), this);
                    mapper = new Mapper<T>(this, klass, entityMapper);
                    Map<Class<?>, Mapper<?>> newMappers = new HashMap<Class<?>, Mapper<?>>(mappers);
                    newMappers.put(klass, mapper);
                    mappers = newMappers;
                }
            }
        }
        return mapper;
    }

    @SuppressWarnings("unchecked")
    <T> TypeCodec<T> getUDTCodec(Class<T> mappedClass) {
        MappedUDTCodec<T> codec = (MappedUDTCodec<T>)udtCodecs.get(mappedClass);
        if (codec == null) {
            synchronized (udtCodecs) {
                codec = (MappedUDTCodec<T>)udtCodecs.get(mappedClass);
                if (codec == null) {
                    codec = AnnotationParser.parseUDT(mappedClass, ReflectionMapper.factory(), this);
                    session.getCluster().getConfiguration().getCodecRegistry().register(codec);

                    HashMap<Class<?>, MappedUDTCodec<?>> newCodecs = new HashMap<Class<?>, MappedUDTCodec<?>>(udtCodecs);
                    newCodecs.put(mappedClass, codec);
                    udtCodecs = newCodecs;
                }
            }
        }
        return codec;
    }

    @SuppressWarnings("unchecked")
    private <T> T getAccessor(Class<T> klass) {
        T accessor = (T)accessors.get(klass);
        if (accessor == null) {
            synchronized (accessors) {
                accessor = (T)accessors.get(klass);
                if (accessor == null) {
                    AccessorMapper<T> mapper = AnnotationParser.parseAccessor(klass, AccessorReflectionMapper.factory(), this);
                    mapper.prepare(this);
                    accessor = mapper.createProxy();
                    Map<Class<?>, Object> newAccessors = new HashMap<Class<?>, Object>(accessors);
                    newAccessors.put(klass, accessor);
                    accessors = newAccessors;
                }
            }
        }
        return accessor;
    }
}
