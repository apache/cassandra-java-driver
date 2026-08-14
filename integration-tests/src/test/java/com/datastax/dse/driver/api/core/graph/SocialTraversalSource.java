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

import java.util.Optional;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectStep;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class SocialTraversalSource extends SocialTraversalSourceDsl {
  public SocialTraversalSource(Graph graph) {
    super(graph);
  }

  public SocialTraversalSource(Graph graph, TraversalStrategies strategies) {
    super(graph, strategies);
  }

  public SocialTraversalSource(RemoteConnection connection) {
    super(connection);
  }

  @Override
  public SocialTraversalSource clone() {
    return (SocialTraversalSource) super.clone();
  }

  @Override
  public SocialTraversalSource with(String key) {
    return (SocialTraversalSource) super.with(key);
  }

  @Override
  public SocialTraversalSource with(String key, Object value) {
    return (SocialTraversalSource) super.with(key, value);
  }

  @Override
  public SocialTraversalSource withStrategies(TraversalStrategy... traversalStrategies) {
    return (SocialTraversalSource) super.withStrategies(traversalStrategies);
  }

  @Override
  public SocialTraversalSource withoutStrategies(
      Class<? extends TraversalStrategy>... traversalStrategyClasses) {
    return (SocialTraversalSource) super.withoutStrategies((Class[]) traversalStrategyClasses);
  }

  @Override
  public SocialTraversalSource withComputer(Computer computer) {
    return (SocialTraversalSource) super.withComputer(computer);
  }

  @Override
  public SocialTraversalSource withComputer(Class<? extends GraphComputer> graphComputerClass) {
    return (SocialTraversalSource) super.withComputer((Class) graphComputerClass);
  }

  @Override
  public SocialTraversalSource withComputer() {
    return (SocialTraversalSource) super.withComputer();
  }

  @Override
  public <A> SocialTraversalSource withSideEffect(
      String key, A initialValue, BinaryOperator<A> reducer) {
    return (SocialTraversalSource)
        super.withSideEffect(key, (Object) initialValue, (BinaryOperator) reducer);
  }

  @Override
  public <A> SocialTraversalSource withSideEffect(String key, A initialValue) {
    return (SocialTraversalSource) super.withSideEffect(key, (Object) initialValue);
  }

  @Override
  public <A> SocialTraversalSource withSideEffect(String key, Supplier<A> initialValue) {
    return (SocialTraversalSource) super.withSideEffect(key, (Supplier) initialValue);
  }

  @Override
  public <A> SocialTraversalSource withSack(
      A initialValue, UnaryOperator<A> splitOperator, BinaryOperator<A> mergeOperator) {
    return (SocialTraversalSource)
        super.withSack(
            (Object) initialValue, (UnaryOperator) splitOperator, (BinaryOperator) mergeOperator);
  }

  @Override
  public <A> SocialTraversalSource withSack(A initialValue) {
    return (SocialTraversalSource) super.withSack((Object) initialValue);
  }

  @Override
  public <A> SocialTraversalSource withSack(Supplier<A> initialValue) {
    return (SocialTraversalSource) super.withSack((Supplier) initialValue);
  }

  @Override
  public <A> SocialTraversalSource withSack(A initialValue, UnaryOperator<A> splitOperator) {
    return (SocialTraversalSource)
        super.withSack((Object) initialValue, (UnaryOperator) splitOperator);
  }

  @Override
  public <A> SocialTraversalSource withSack(A initialValue, BinaryOperator<A> mergeOperator) {
    return (SocialTraversalSource)
        super.withSack((Object) initialValue, (BinaryOperator) mergeOperator);
  }

  @Override
  public SocialTraversalSource withBulk(boolean useBulk) {
    return (SocialTraversalSource) super.withBulk(useBulk);
  }

  @Override
  public SocialTraversalSource withPath() {
    return (SocialTraversalSource) super.withPath();
  }

  public SocialTraversal<Vertex, Vertex> persons(String... names) {
    SocialTraversalSource clone = this.clone();
    return new DefaultSocialTraversal<Vertex, Vertex>(
        clone, (GraphTraversal.Admin) super.persons(names).asAdmin());
  }

  public SocialTraversal<Vertex, Vertex> addV() {
    SocialTraversalSource clone = this.clone();
    clone.getBytecode().addStep("addV", new Object[0]);
    DefaultSocialTraversal traversal = new DefaultSocialTraversal(clone);
    return (SocialTraversal)
        traversal.asAdmin().addStep((Step) new AddVertexStartStep(traversal, (String) null));
  }

  public SocialTraversal<Vertex, Vertex> addV(String label) {
    SocialTraversalSource clone = this.clone();
    clone.getBytecode().addStep("addV", label);
    DefaultSocialTraversal traversal = new DefaultSocialTraversal(clone);
    return (SocialTraversal)
        traversal.asAdmin().addStep((Step) new AddVertexStartStep(traversal, label));
  }

  public SocialTraversal<Vertex, Vertex> addV(Traversal vertexLabelTraversal) {
    SocialTraversalSource clone = this.clone();
    clone.getBytecode().addStep("addV", vertexLabelTraversal);
    DefaultSocialTraversal traversal = new DefaultSocialTraversal(clone);
    return (SocialTraversal)
        traversal.asAdmin().addStep((Step) new AddVertexStartStep(traversal, vertexLabelTraversal));
  }

  public SocialTraversal<Edge, Edge> addE(String label) {
    SocialTraversalSource clone = this.clone();
    clone.getBytecode().addStep("addE", label);
    DefaultSocialTraversal traversal = new DefaultSocialTraversal(clone);
    return (SocialTraversal)
        traversal.asAdmin().addStep((Step) new AddEdgeStartStep(traversal, label));
  }

  public SocialTraversal<Edge, Edge> addE(Traversal edgeLabelTraversal) {
    SocialTraversalSource clone = this.clone();
    clone.getBytecode().addStep("addE", edgeLabelTraversal);
    DefaultSocialTraversal traversal = new DefaultSocialTraversal(clone);
    return (SocialTraversal)
        traversal.asAdmin().addStep((Step) new AddEdgeStartStep(traversal, edgeLabelTraversal));
  }

  public SocialTraversal<Vertex, Vertex> V(Object... vertexIds) {
    SocialTraversalSource clone = this.clone();
    clone.getBytecode().addStep("V", vertexIds);
    DefaultSocialTraversal traversal = new DefaultSocialTraversal(clone);
    return (SocialTraversal)
        traversal.asAdmin().addStep((Step) new GraphStep(traversal, Vertex.class, true, vertexIds));
  }

  public SocialTraversal<Edge, Edge> E(Object... edgeIds) {
    SocialTraversalSource clone = this.clone();
    clone.getBytecode().addStep("E", edgeIds);
    DefaultSocialTraversal traversal = new DefaultSocialTraversal(clone);
    return (SocialTraversal)
        traversal.asAdmin().addStep((Step) new GraphStep(traversal, Edge.class, true, edgeIds));
  }

  public <S> SocialTraversal<S, S> inject(S... starts) {
    SocialTraversalSource clone = this.clone();
    clone.getBytecode().addStep("inject", starts);
    DefaultSocialTraversal traversal = new DefaultSocialTraversal(clone);
    return (SocialTraversal)
        traversal.asAdmin().addStep(new InjectStep<S>((Traversal.Admin) traversal, starts));
  }

  @Override
  public Optional<Class<?>> getAnonymousTraversalClass() {
    return Optional.of(__.class);
  }
}
