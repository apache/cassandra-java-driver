/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.dse.driver.api.core.graph;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

public interface SocialTraversal<S, E> extends SocialTraversalDsl<S, E> {
  @Override
  public default SocialTraversal<S, Vertex> knows(String personName) {
    return (SocialTraversal) SocialTraversalDsl.super.knows(personName);
  }

  @Override
  public default <E2> SocialTraversal<S, E2> map(Function<Traverser<E>, E2> function) {
    return (SocialTraversal) SocialTraversalDsl.super.map(function);
  }

  @Override
  public default <E2> SocialTraversal<S, E2> map(Traversal<?, E2> mapTraversal) {
    return (SocialTraversal) SocialTraversalDsl.super.map(mapTraversal);
  }

  @Override
  public default <E2> SocialTraversal<S, E2> flatMap(
      Function<Traverser<E>, Iterator<E2>> function) {
    return (SocialTraversal) SocialTraversalDsl.super.flatMap(function);
  }

  @Override
  public default <E2> SocialTraversal<S, E2> flatMap(Traversal<?, E2> flatMapTraversal) {
    return (SocialTraversal) SocialTraversalDsl.super.flatMap(flatMapTraversal);
  }

  @Override
  public default SocialTraversal<S, Object> id() {
    return (SocialTraversal) SocialTraversalDsl.super.id();
  }

  @Override
  public default SocialTraversal<S, String> label() {
    return (SocialTraversal) SocialTraversalDsl.super.label();
  }

  @Override
  public default SocialTraversal<S, E> identity() {
    return (SocialTraversal) SocialTraversalDsl.super.identity();
  }

  @Override
  public default <E2> SocialTraversal<S, E2> constant(E2 e) {
    return (SocialTraversal) SocialTraversalDsl.super.constant(e);
  }

  @Override
  public default SocialTraversal<S, Vertex> V(Object... vertexIdsOrElements) {
    return (SocialTraversal) SocialTraversalDsl.super.V(vertexIdsOrElements);
  }

  @Override
  public default SocialTraversal<S, Vertex> to(Direction direction, String... edgeLabels) {
    return (SocialTraversal) SocialTraversalDsl.super.to(direction, edgeLabels);
  }

  @Override
  public default SocialTraversal<S, Vertex> out(String... edgeLabels) {
    return (SocialTraversal) SocialTraversalDsl.super.out(edgeLabels);
  }

  @Override
  public default SocialTraversal<S, Vertex> in(String... edgeLabels) {
    return (SocialTraversal) SocialTraversalDsl.super.in(edgeLabels);
  }

  @Override
  public default SocialTraversal<S, Vertex> both(String... edgeLabels) {
    return (SocialTraversal) SocialTraversalDsl.super.both(edgeLabels);
  }

  @Override
  public default SocialTraversal<S, Edge> toE(Direction direction, String... edgeLabels) {
    return (SocialTraversal) SocialTraversalDsl.super.toE(direction, edgeLabels);
  }

  @Override
  public default SocialTraversal<S, Edge> outE(String... edgeLabels) {
    return (SocialTraversal) SocialTraversalDsl.super.outE(edgeLabels);
  }

  @Override
  public default SocialTraversal<S, Edge> inE(String... edgeLabels) {
    return (SocialTraversal) SocialTraversalDsl.super.inE(edgeLabels);
  }

  @Override
  public default SocialTraversal<S, Edge> bothE(String... edgeLabels) {
    return (SocialTraversal) SocialTraversalDsl.super.bothE(edgeLabels);
  }

  @Override
  public default SocialTraversal<S, Vertex> toV(Direction direction) {
    return (SocialTraversal) SocialTraversalDsl.super.toV(direction);
  }

  @Override
  public default SocialTraversal<S, Vertex> inV() {
    return (SocialTraversal) SocialTraversalDsl.super.inV();
  }

  @Override
  public default SocialTraversal<S, Vertex> outV() {
    return (SocialTraversal) SocialTraversalDsl.super.outV();
  }

  @Override
  public default SocialTraversal<S, Vertex> bothV() {
    return (SocialTraversal) SocialTraversalDsl.super.bothV();
  }

  @Override
  public default SocialTraversal<S, Vertex> otherV() {
    return (SocialTraversal) SocialTraversalDsl.super.otherV();
  }

  @Override
  public default SocialTraversal<S, E> order() {
    return (SocialTraversal) SocialTraversalDsl.super.order();
  }

  @Override
  public default SocialTraversal<S, E> order(Scope scope) {
    return (SocialTraversal) SocialTraversalDsl.super.order(scope);
  }

  @Override
  public default <E2> SocialTraversal<S, ? extends Property<E2>> properties(
      String... propertyKeys) {
    return (SocialTraversal) SocialTraversalDsl.super.properties(propertyKeys);
  }

  @Override
  public default <E2> SocialTraversal<S, E2> values(String... propertyKeys) {
    return (SocialTraversal) SocialTraversalDsl.super.values(propertyKeys);
  }

  @Override
  public default <E2> SocialTraversal<S, Map<String, E2>> propertyMap(String... propertyKeys) {
    return (SocialTraversal) SocialTraversalDsl.super.propertyMap(propertyKeys);
  }

  @Override
  public default <E2> SocialTraversal<S, Map<Object, E2>> elementMap(String... propertyKeys) {
    return (SocialTraversal) SocialTraversalDsl.super.elementMap(propertyKeys);
  }

  @Override
  public default <E2> SocialTraversal<S, Map<Object, E2>> valueMap(String... propertyKeys) {
    return (SocialTraversal) SocialTraversalDsl.super.valueMap(propertyKeys);
  }

  @Override
  public default <E2> SocialTraversal<S, Map<Object, E2>> valueMap(
      boolean includeTokens, String... propertyKeys) {
    return (SocialTraversal) SocialTraversalDsl.super.valueMap(includeTokens, propertyKeys);
  }

  @Override
  public default SocialTraversal<S, String> key() {
    return (SocialTraversal) SocialTraversalDsl.super.key();
  }

  @Override
  public default <E2> SocialTraversal<S, E2> value() {
    return (SocialTraversal) SocialTraversalDsl.super.value();
  }

  @Override
  public default SocialTraversal<S, Path> path() {
    return (SocialTraversal) SocialTraversalDsl.super.path();
  }

  @Override
  public default <E2> SocialTraversal<S, Map<String, E2>> match(
      Traversal<?, ?>... matchTraversals) {
    return (SocialTraversal) SocialTraversalDsl.super.match(matchTraversals);
  }

  @Override
  public default <E2> SocialTraversal<S, E2> sack() {
    return (SocialTraversal) SocialTraversalDsl.super.sack();
  }

  @Override
  public default SocialTraversal<S, Integer> loops() {
    return (SocialTraversal) SocialTraversalDsl.super.loops();
  }

  @Override
  public default SocialTraversal<S, Integer> loops(String loopName) {
    return (SocialTraversal) SocialTraversalDsl.super.loops(loopName);
  }

  @Override
  public default <E2> SocialTraversal<S, Map<String, E2>> project(
      String projectKey, String... otherProjectKeys) {
    return (SocialTraversal) SocialTraversalDsl.super.project(projectKey, otherProjectKeys);
  }

  @Override
  public default <E2> SocialTraversal<S, Map<String, E2>> select(
      Pop pop, String selectKey1, String selectKey2, String... otherSelectKeys) {
    return (SocialTraversal)
        SocialTraversalDsl.super.select(pop, selectKey1, selectKey2, otherSelectKeys);
  }

  @Override
  public default <E2> SocialTraversal<S, Map<String, E2>> select(
      String selectKey1, String selectKey2, String... otherSelectKeys) {
    return (SocialTraversal)
        SocialTraversalDsl.super.select(selectKey1, selectKey2, otherSelectKeys);
  }

  @Override
  public default <E2> SocialTraversal<S, E2> select(Pop pop, String selectKey) {
    return (SocialTraversal) SocialTraversalDsl.super.select(pop, selectKey);
  }

  @Override
  public default <E2> SocialTraversal<S, E2> select(String selectKey) {
    return (SocialTraversal) SocialTraversalDsl.super.select(selectKey);
  }

  @Override
  public default <E2> SocialTraversal<S, E2> select(Pop pop, Traversal<S, E2> keyTraversal) {
    return (SocialTraversal) SocialTraversalDsl.super.select(pop, keyTraversal);
  }

  @Override
  public default <E2> SocialTraversal<S, E2> select(Traversal<S, E2> keyTraversal) {
    return (SocialTraversal) SocialTraversalDsl.super.select(keyTraversal);
  }

  @Override
  public default <E2> SocialTraversal<S, Collection<E2>> select(Column column) {
    return (SocialTraversal) SocialTraversalDsl.super.select(column);
  }

  @Override
  public default <E2> SocialTraversal<S, E2> unfold() {
    return (SocialTraversal) SocialTraversalDsl.super.unfold();
  }

  @Override
  public default SocialTraversal<S, List<E>> fold() {
    return (SocialTraversal) SocialTraversalDsl.super.fold();
  }

  @Override
  public default <E2> SocialTraversal<S, E2> fold(E2 seed, BiFunction<E2, E, E2> foldFunction) {
    return (SocialTraversal) SocialTraversalDsl.super.fold(seed, foldFunction);
  }

  @Override
  public default SocialTraversal<S, Long> count() {
    return (SocialTraversal) SocialTraversalDsl.super.count();
  }

  @Override
  public default SocialTraversal<S, Long> count(Scope scope) {
    return (SocialTraversal) SocialTraversalDsl.super.count(scope);
  }

  @Override
  public default <E2 extends Number> SocialTraversal<S, E2> sum() {
    return (SocialTraversal) SocialTraversalDsl.super.sum();
  }

  @Override
  public default <E2 extends Number> SocialTraversal<S, E2> sum(Scope scope) {
    return (SocialTraversal) SocialTraversalDsl.super.sum(scope);
  }

  @Override
  public default <E2 extends Comparable> SocialTraversal<S, E2> max() {
    return (SocialTraversal) SocialTraversalDsl.super.max();
  }

  @Override
  public default <E2 extends Comparable> SocialTraversal<S, E2> max(Scope scope) {
    return (SocialTraversal) SocialTraversalDsl.super.max(scope);
  }

  @Override
  public default <E2 extends Comparable> SocialTraversal<S, E2> min() {
    return (SocialTraversal) SocialTraversalDsl.super.min();
  }

  @Override
  public default <E2 extends Comparable> SocialTraversal<S, E2> min(Scope scope) {
    return (SocialTraversal) SocialTraversalDsl.super.min(scope);
  }

  @Override
  public default <E2 extends Number> SocialTraversal<S, E2> mean() {
    return (SocialTraversal) SocialTraversalDsl.super.mean();
  }

  @Override
  public default <E2 extends Number> SocialTraversal<S, E2> mean(Scope scope) {
    return (SocialTraversal) SocialTraversalDsl.super.mean(scope);
  }

  @Override
  public default <K, V> SocialTraversal<S, Map<K, V>> group() {
    return (SocialTraversal) SocialTraversalDsl.super.group();
  }

  @Override
  public default <K> SocialTraversal<S, Map<K, Long>> groupCount() {
    return (SocialTraversal) SocialTraversalDsl.super.groupCount();
  }

  @Override
  public default SocialTraversal<S, Tree> tree() {
    return (SocialTraversal) SocialTraversalDsl.super.tree();
  }

  @Override
  public default SocialTraversal<S, Vertex> addV(String vertexLabel) {
    return (SocialTraversal) SocialTraversalDsl.super.addV(vertexLabel);
  }

  @Override
  public default SocialTraversal<S, Vertex> addV(Traversal<?, String> vertexLabelTraversal) {
    return (SocialTraversal) SocialTraversalDsl.super.addV(vertexLabelTraversal);
  }

  @Override
  public default SocialTraversal<S, Vertex> addV() {
    return (SocialTraversal) SocialTraversalDsl.super.addV();
  }

  @Override
  public default SocialTraversal<S, Edge> addE(String edgeLabel) {
    return (SocialTraversal) SocialTraversalDsl.super.addE(edgeLabel);
  }

  @Override
  public default SocialTraversal<S, Edge> addE(Traversal<?, String> edgeLabelTraversal) {
    return (SocialTraversal) SocialTraversalDsl.super.addE(edgeLabelTraversal);
  }

  @Override
  public default SocialTraversal<S, E> to(String toStepLabel) {
    return (SocialTraversal) SocialTraversalDsl.super.to(toStepLabel);
  }

  @Override
  public default SocialTraversal<S, E> from(String fromStepLabel) {
    return (SocialTraversal) SocialTraversalDsl.super.from(fromStepLabel);
  }

  @Override
  public default SocialTraversal<S, E> to(Traversal<?, Vertex> toVertex) {
    return (SocialTraversal) SocialTraversalDsl.super.to(toVertex);
  }

  @Override
  public default SocialTraversal<S, E> from(Traversal<?, Vertex> fromVertex) {
    return (SocialTraversal) SocialTraversalDsl.super.from(fromVertex);
  }

  @Override
  public default SocialTraversal<S, E> to(Vertex toVertex) {
    return (SocialTraversal) SocialTraversalDsl.super.to(toVertex);
  }

  @Override
  public default SocialTraversal<S, E> from(Vertex fromVertex) {
    return (SocialTraversal) SocialTraversalDsl.super.from(fromVertex);
  }

  @Override
  public default SocialTraversal<S, Double> math(String expression) {
    return (SocialTraversal) SocialTraversalDsl.super.math(expression);
  }

  @Override
  public default SocialTraversal<S, E> filter(Predicate<Traverser<E>> predicate) {
    return (SocialTraversal) SocialTraversalDsl.super.filter(predicate);
  }

  @Override
  public default SocialTraversal<S, E> filter(Traversal<?, ?> filterTraversal) {
    return (SocialTraversal) SocialTraversalDsl.super.filter(filterTraversal);
  }

  @Override
  public default SocialTraversal<S, E> or(Traversal<?, ?>... orTraversals) {
    return (SocialTraversal) SocialTraversalDsl.super.or(orTraversals);
  }

  @Override
  public default SocialTraversal<S, E> and(Traversal<?, ?>... andTraversals) {
    return (SocialTraversal) SocialTraversalDsl.super.and(andTraversals);
  }

  @Override
  public default SocialTraversal<S, E> inject(E... injections) {
    return (SocialTraversal) SocialTraversalDsl.super.inject(injections);
  }

  @Override
  public default SocialTraversal<S, E> dedup(Scope scope, String... dedupLabels) {
    return (SocialTraversal) SocialTraversalDsl.super.dedup(scope, dedupLabels);
  }

  @Override
  public default SocialTraversal<S, E> dedup(String... dedupLabels) {
    return (SocialTraversal) SocialTraversalDsl.super.dedup(dedupLabels);
  }

  @Override
  public default SocialTraversal<S, E> where(String startKey, P<String> predicate) {
    return (SocialTraversal) SocialTraversalDsl.super.where(startKey, predicate);
  }

  @Override
  public default SocialTraversal<S, E> where(P<String> predicate) {
    return (SocialTraversal) SocialTraversalDsl.super.where(predicate);
  }

  @Override
  public default SocialTraversal<S, E> where(Traversal<?, ?> whereTraversal) {
    return (SocialTraversal) SocialTraversalDsl.super.where(whereTraversal);
  }

  @Override
  public default SocialTraversal<S, E> has(String propertyKey, P<?> predicate) {
    return (SocialTraversal) SocialTraversalDsl.super.has(propertyKey, predicate);
  }

  @Override
  public default SocialTraversal<S, E> has(T accessor, P<?> predicate) {
    return (SocialTraversal) SocialTraversalDsl.super.has(accessor, predicate);
  }

  @Override
  public default SocialTraversal<S, E> has(String propertyKey, Object value) {
    return (SocialTraversal) SocialTraversalDsl.super.has(propertyKey, value);
  }

  @Override
  public default SocialTraversal<S, E> has(T accessor, Object value) {
    return (SocialTraversal) SocialTraversalDsl.super.has(accessor, value);
  }

  @Override
  public default SocialTraversal<S, E> has(String label, String propertyKey, P<?> predicate) {
    return (SocialTraversal) SocialTraversalDsl.super.has(label, propertyKey, predicate);
  }

  @Override
  public default SocialTraversal<S, E> has(String label, String propertyKey, Object value) {
    return (SocialTraversal) SocialTraversalDsl.super.has(label, propertyKey, value);
  }

  @Override
  public default SocialTraversal<S, E> has(T accessor, Traversal<?, ?> propertyTraversal) {
    return (SocialTraversal) SocialTraversalDsl.super.has(accessor, propertyTraversal);
  }

  @Override
  public default SocialTraversal<S, E> has(String propertyKey, Traversal<?, ?> propertyTraversal) {
    return (SocialTraversal) SocialTraversalDsl.super.has(propertyKey, propertyTraversal);
  }

  @Override
  public default SocialTraversal<S, E> has(String propertyKey) {
    return (SocialTraversal) SocialTraversalDsl.super.has(propertyKey);
  }

  @Override
  public default SocialTraversal<S, E> hasNot(String propertyKey) {
    return (SocialTraversal) SocialTraversalDsl.super.hasNot(propertyKey);
  }

  @Override
  public default SocialTraversal<S, E> hasLabel(String label, String... otherLabels) {
    return (SocialTraversal) SocialTraversalDsl.super.hasLabel(label, otherLabels);
  }

  @Override
  public default SocialTraversal<S, E> hasLabel(P<String> predicate) {
    return (SocialTraversal) SocialTraversalDsl.super.hasLabel(predicate);
  }

  @Override
  public default SocialTraversal<S, E> hasId(Object id, Object... otherIds) {
    return (SocialTraversal) SocialTraversalDsl.super.hasId(id, otherIds);
  }

  @Override
  public default SocialTraversal<S, E> hasId(P<Object> predicate) {
    return (SocialTraversal) SocialTraversalDsl.super.hasId(predicate);
  }

  @Override
  public default SocialTraversal<S, E> hasKey(String label, String... otherLabels) {
    return (SocialTraversal) SocialTraversalDsl.super.hasKey(label, otherLabels);
  }

  @Override
  public default SocialTraversal<S, E> hasKey(P<String> predicate) {
    return (SocialTraversal) SocialTraversalDsl.super.hasKey(predicate);
  }

  @Override
  public default SocialTraversal<S, E> hasValue(Object value, Object... otherValues) {
    return (SocialTraversal) SocialTraversalDsl.super.hasValue(value, otherValues);
  }

  @Override
  public default SocialTraversal<S, E> hasValue(P<Object> predicate) {
    return (SocialTraversal) SocialTraversalDsl.super.hasValue(predicate);
  }

  @Override
  public default SocialTraversal<S, E> is(P<E> predicate) {
    return (SocialTraversal) SocialTraversalDsl.super.is(predicate);
  }

  @Override
  public default SocialTraversal<S, E> is(Object value) {
    return (SocialTraversal) SocialTraversalDsl.super.is(value);
  }

  @Override
  public default SocialTraversal<S, E> not(Traversal<?, ?> notTraversal) {
    return (SocialTraversal) SocialTraversalDsl.super.not(notTraversal);
  }

  @Override
  public default SocialTraversal<S, E> coin(double probability) {
    return (SocialTraversal) SocialTraversalDsl.super.coin(probability);
  }

  @Override
  public default SocialTraversal<S, E> range(long low, long high) {
    return (SocialTraversal) SocialTraversalDsl.super.range(low, high);
  }

  @Override
  public default <E2> SocialTraversal<S, E2> range(Scope scope, long low, long high) {
    return (SocialTraversal) SocialTraversalDsl.super.range(scope, low, high);
  }

  @Override
  public default SocialTraversal<S, E> limit(long limit) {
    return (SocialTraversal) SocialTraversalDsl.super.limit(limit);
  }

  @Override
  public default <E2> SocialTraversal<S, E2> limit(Scope scope, long limit) {
    return (SocialTraversal) SocialTraversalDsl.super.limit(scope, limit);
  }

  @Override
  public default SocialTraversal<S, E> tail() {
    return (SocialTraversal) SocialTraversalDsl.super.tail();
  }

  @Override
  public default SocialTraversal<S, E> tail(long limit) {
    return (SocialTraversal) SocialTraversalDsl.super.tail(limit);
  }

  @Override
  public default <E2> SocialTraversal<S, E2> tail(Scope scope) {
    return (SocialTraversal) SocialTraversalDsl.super.tail(scope);
  }

  @Override
  public default <E2> SocialTraversal<S, E2> tail(Scope scope, long limit) {
    return (SocialTraversal) SocialTraversalDsl.super.tail(scope, limit);
  }

  @Override
  public default SocialTraversal<S, E> skip(long skip) {
    return (SocialTraversal) SocialTraversalDsl.super.skip(skip);
  }

  @Override
  public default <E2> SocialTraversal<S, E2> skip(Scope scope, long skip) {
    return (SocialTraversal) SocialTraversalDsl.super.skip(scope, skip);
  }

  @Override
  public default SocialTraversal<S, E> timeLimit(long timeLimit) {
    return (SocialTraversal) SocialTraversalDsl.super.timeLimit(timeLimit);
  }

  @Override
  public default SocialTraversal<S, E> simplePath() {
    return (SocialTraversal) SocialTraversalDsl.super.simplePath();
  }

  @Override
  public default SocialTraversal<S, E> cyclicPath() {
    return (SocialTraversal) SocialTraversalDsl.super.cyclicPath();
  }

  @Override
  public default SocialTraversal<S, E> sample(int amountToSample) {
    return (SocialTraversal) SocialTraversalDsl.super.sample(amountToSample);
  }

  @Override
  public default SocialTraversal<S, E> sample(Scope scope, int amountToSample) {
    return (SocialTraversal) SocialTraversalDsl.super.sample(scope, amountToSample);
  }

  @Override
  public default SocialTraversal<S, E> drop() {
    return (SocialTraversal) SocialTraversalDsl.super.drop();
  }

  @Override
  public default SocialTraversal<S, E> sideEffect(Consumer<Traverser<E>> consumer) {
    return (SocialTraversal) SocialTraversalDsl.super.sideEffect(consumer);
  }

  @Override
  public default SocialTraversal<S, E> sideEffect(Traversal<?, ?> sideEffectTraversal) {
    return (SocialTraversal) SocialTraversalDsl.super.sideEffect(sideEffectTraversal);
  }

  @Override
  public default <E2> SocialTraversal<S, E2> cap(String sideEffectKey, String... sideEffectKeys) {
    return (SocialTraversal) SocialTraversalDsl.super.cap(sideEffectKey, sideEffectKeys);
  }

  @Override
  public default SocialTraversal<S, Edge> subgraph(String sideEffectKey) {
    return (SocialTraversal) SocialTraversalDsl.super.subgraph(sideEffectKey);
  }

  @Override
  public default SocialTraversal<S, E> aggregate(String sideEffectKey) {
    return (SocialTraversal) SocialTraversalDsl.super.aggregate(sideEffectKey);
  }

  @Override
  public default SocialTraversal<S, E> aggregate(Scope scope, String sideEffectKey) {
    return (SocialTraversal) SocialTraversalDsl.super.aggregate(scope, sideEffectKey);
  }

  @Override
  public default SocialTraversal<S, E> group(String sideEffectKey) {
    return (SocialTraversal) SocialTraversalDsl.super.group(sideEffectKey);
  }

  @Override
  public default SocialTraversal<S, E> groupCount(String sideEffectKey) {
    return (SocialTraversal) SocialTraversalDsl.super.groupCount(sideEffectKey);
  }

  @Override
  public default SocialTraversal<S, E> tree(String sideEffectKey) {
    return (SocialTraversal) SocialTraversalDsl.super.tree(sideEffectKey);
  }

  @Override
  public default <V, U> SocialTraversal<S, E> sack(BiFunction<V, U, V> sackOperator) {
    return (SocialTraversal) SocialTraversalDsl.super.sack(sackOperator);
  }

  @Override
  public default SocialTraversal<S, E> store(String sideEffectKey) {
    return (SocialTraversal) SocialTraversalDsl.super.store(sideEffectKey);
  }

  @Override
  public default SocialTraversal<S, E> profile(String sideEffectKey) {
    return (SocialTraversal) SocialTraversalDsl.super.profile(sideEffectKey);
  }

  @Override
  public default SocialTraversal<S, TraversalMetrics> profile() {
    return (SocialTraversal) SocialTraversalDsl.super.profile();
  }

  @Override
  public default SocialTraversal<S, E> none() {
    return (SocialTraversal) SocialTraversalDsl.super.none();
  }

  @Override
  public default SocialTraversal<S, E> property(
      VertexProperty.Cardinality cardinality, Object key, Object value, Object... keyValues) {
    return (SocialTraversal) SocialTraversalDsl.super.property(cardinality, key, value, keyValues);
  }

  @Override
  public default SocialTraversal<S, E> property(Object key, Object value, Object... keyValues) {
    return (SocialTraversal) SocialTraversalDsl.super.property(key, value, keyValues);
  }

  @Override
  public default <M, E2> SocialTraversal<S, E2> branch(Traversal<?, M> branchTraversal) {
    return (SocialTraversal) SocialTraversalDsl.super.branch(branchTraversal);
  }

  @Override
  public default <M, E2> SocialTraversal<S, E2> branch(Function<Traverser<E>, M> function) {
    return (SocialTraversal) SocialTraversalDsl.super.branch(function);
  }

  @Override
  public default <M, E2> SocialTraversal<S, E2> choose(Traversal<?, M> choiceTraversal) {
    return (SocialTraversal) SocialTraversalDsl.super.choose(choiceTraversal);
  }

  @Override
  public default <E2> SocialTraversal<S, E2> choose(
      Traversal<?, ?> traversalPredicate,
      Traversal<?, E2> trueChoice,
      Traversal<?, E2> falseChoice) {
    return (SocialTraversal)
        SocialTraversalDsl.super.choose(traversalPredicate, trueChoice, falseChoice);
  }

  @Override
  public default <E2> SocialTraversal<S, E2> choose(
      Traversal<?, ?> traversalPredicate, Traversal<?, E2> trueChoice) {
    return (SocialTraversal) SocialTraversalDsl.super.choose(traversalPredicate, trueChoice);
  }

  @Override
  public default <M, E2> SocialTraversal<S, E2> choose(Function<E, M> choiceFunction) {
    return (SocialTraversal) SocialTraversalDsl.super.choose(choiceFunction);
  }

  @Override
  public default <E2> SocialTraversal<S, E2> choose(
      Predicate<E> choosePredicate, Traversal<?, E2> trueChoice, Traversal<?, E2> falseChoice) {
    return (SocialTraversal)
        SocialTraversalDsl.super.choose(choosePredicate, trueChoice, falseChoice);
  }

  @Override
  public default <E2> SocialTraversal<S, E2> choose(
      Predicate<E> choosePredicate, Traversal<?, E2> trueChoice) {
    return (SocialTraversal) SocialTraversalDsl.super.choose(choosePredicate, trueChoice);
  }

  @Override
  public default <E2> SocialTraversal<S, E2> optional(Traversal<?, E2> optionalTraversal) {
    return (SocialTraversal) SocialTraversalDsl.super.optional(optionalTraversal);
  }

  @Override
  public default <E2> SocialTraversal<S, E2> union(Traversal<?, E2>... unionTraversals) {
    return (SocialTraversal) SocialTraversalDsl.super.union(unionTraversals);
  }

  @Override
  public default <E2> SocialTraversal<S, E2> coalesce(Traversal<?, E2>... coalesceTraversals) {
    return (SocialTraversal) SocialTraversalDsl.super.coalesce(coalesceTraversals);
  }

  @Override
  public default SocialTraversal<S, E> repeat(Traversal<?, E> repeatTraversal) {
    return (SocialTraversal) SocialTraversalDsl.super.repeat(repeatTraversal);
  }

  @Override
  public default SocialTraversal<S, E> repeat(String loopName, Traversal<?, E> repeatTraversal) {
    return (SocialTraversal) SocialTraversalDsl.super.repeat(loopName, repeatTraversal);
  }

  @Override
  public default SocialTraversal<S, E> emit(Traversal<?, ?> emitTraversal) {
    return (SocialTraversal) SocialTraversalDsl.super.emit(emitTraversal);
  }

  @Override
  public default SocialTraversal<S, E> emit(Predicate<Traverser<E>> emitPredicate) {
    return (SocialTraversal) SocialTraversalDsl.super.emit(emitPredicate);
  }

  @Override
  public default SocialTraversal<S, E> emit() {
    return (SocialTraversal) SocialTraversalDsl.super.emit();
  }

  @Override
  public default SocialTraversal<S, E> until(Traversal<?, ?> untilTraversal) {
    return (SocialTraversal) SocialTraversalDsl.super.until(untilTraversal);
  }

  @Override
  public default SocialTraversal<S, E> until(Predicate<Traverser<E>> untilPredicate) {
    return (SocialTraversal) SocialTraversalDsl.super.until(untilPredicate);
  }

  @Override
  public default SocialTraversal<S, E> times(int maxLoops) {
    return (SocialTraversal) SocialTraversalDsl.super.times(maxLoops);
  }

  @Override
  public default <E2> SocialTraversal<S, E2> local(Traversal<?, E2> localTraversal) {
    return (SocialTraversal) SocialTraversalDsl.super.local(localTraversal);
  }

  @Override
  public default SocialTraversal<S, E> pageRank() {
    return (SocialTraversal) SocialTraversalDsl.super.pageRank();
  }

  @Override
  public default SocialTraversal<S, E> pageRank(double alpha) {
    return (SocialTraversal) SocialTraversalDsl.super.pageRank(alpha);
  }

  @Override
  public default SocialTraversal<S, E> peerPressure() {
    return (SocialTraversal) SocialTraversalDsl.super.peerPressure();
  }

  @Override
  public default SocialTraversal<S, E> connectedComponent() {
    return (SocialTraversal) SocialTraversalDsl.super.connectedComponent();
  }

  @Override
  public default SocialTraversal<S, Path> shortestPath() {
    return (SocialTraversal) SocialTraversalDsl.super.shortestPath();
  }

  @Override
  public default SocialTraversal<S, E> program(VertexProgram<?> vertexProgram) {
    return (SocialTraversal) SocialTraversalDsl.super.program(vertexProgram);
  }

  @Override
  public default SocialTraversal<S, E> as(String stepLabel, String... stepLabels) {
    return (SocialTraversal) SocialTraversalDsl.super.as(stepLabel, stepLabels);
  }

  @Override
  public default SocialTraversal<S, E> barrier() {
    return (SocialTraversal) SocialTraversalDsl.super.barrier();
  }

  @Override
  public default SocialTraversal<S, E> barrier(int maxBarrierSize) {
    return (SocialTraversal) SocialTraversalDsl.super.barrier(maxBarrierSize);
  }

  @Override
  public default <E2> SocialTraversal<S, E2> index() {
    return (SocialTraversal) SocialTraversalDsl.super.index();
  }

  @Override
  public default SocialTraversal<S, E> barrier(Consumer<TraverserSet<Object>> barrierConsumer) {
    return (SocialTraversal) SocialTraversalDsl.super.barrier(barrierConsumer);
  }

  @Override
  public default SocialTraversal<S, E> with(String key) {
    return (SocialTraversal) SocialTraversalDsl.super.with(key);
  }

  @Override
  public default SocialTraversal<S, E> with(String key, Object value) {
    return (SocialTraversal) SocialTraversalDsl.super.with(key, value);
  }

  @Override
  public default SocialTraversal<S, E> by() {
    return (SocialTraversal) SocialTraversalDsl.super.by();
  }

  @Override
  public default SocialTraversal<S, E> by(Traversal<?, ?> traversal) {
    return (SocialTraversal) SocialTraversalDsl.super.by(traversal);
  }

  @Override
  public default SocialTraversal<S, E> by(T token) {
    return (SocialTraversal) SocialTraversalDsl.super.by(token);
  }

  @Override
  public default SocialTraversal<S, E> by(String key) {
    return (SocialTraversal) SocialTraversalDsl.super.by(key);
  }

  @Override
  public default <V> SocialTraversal<S, E> by(Function<V, Object> function) {
    return (SocialTraversal) SocialTraversalDsl.super.by(function);
  }

  @Override
  public default <V> SocialTraversal<S, E> by(Traversal<?, ?> traversal, Comparator<V> comparator) {
    return (SocialTraversal) SocialTraversalDsl.super.by(traversal, comparator);
  }

  @Override
  public default SocialTraversal<S, E> by(Comparator<E> comparator) {
    return (SocialTraversal) SocialTraversalDsl.super.by(comparator);
  }

  @Override
  public default SocialTraversal<S, E> by(Order order) {
    return (SocialTraversal) SocialTraversalDsl.super.by(order);
  }

  @Override
  public default <V> SocialTraversal<S, E> by(String key, Comparator<V> comparator) {
    return (SocialTraversal) SocialTraversalDsl.super.by(key, comparator);
  }

  @Override
  public default <U> SocialTraversal<S, E> by(Function<U, Object> function, Comparator comparator) {
    return (SocialTraversal) SocialTraversalDsl.super.by(function, comparator);
  }

  @Override
  public default <M, E2> SocialTraversal<S, E> option(M pick, Traversal<?, E2> traversalOption) {
    return (SocialTraversal) SocialTraversalDsl.super.option(pick, traversalOption);
  }

  @Override
  public default <E2> SocialTraversal<S, E> option(Traversal<?, E2> traversalOption) {
    return (SocialTraversal) SocialTraversalDsl.super.option(traversalOption);
  }

  @Override
  public default SocialTraversal<S, E> read() {
    return (SocialTraversal) SocialTraversalDsl.super.read();
  }

  @Override
  public default SocialTraversal<S, E> write() {
    return (SocialTraversal) SocialTraversalDsl.super.write();
  }

  @Override
  public default SocialTraversal<S, E> iterate() {
    SocialTraversalDsl.super.iterate();
    return this;
  }
}
