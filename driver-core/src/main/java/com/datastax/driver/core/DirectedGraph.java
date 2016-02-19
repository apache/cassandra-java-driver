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
package com.datastax.driver.core;

import com.datastax.driver.core.exceptions.DriverInternalError;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import java.util.*;

/**
 * A basic directed graph implementation to perform topological sorts.
 */
class DirectedGraph<V> {

    // We need to keep track of the predecessor count. For simplicity, use a map to store it alongside the vertices.
    final Map<V, Integer> vertices;
    final Multimap<V, V> adjacencyList;
    boolean wasSorted;

    DirectedGraph(List<V> vertices) {
        this.vertices = Maps.newHashMapWithExpectedSize(vertices.size());
        this.adjacencyList = HashMultimap.create();

        for (V vertex : vertices) {
            this.vertices.put(vertex, 0);
        }
    }

    DirectedGraph(V... vertices) {
        this(Arrays.asList(vertices));
    }

    /**
     * this assumes that {@code from} and {@code to} were part of the vertices passed to the constructor
     */
    void addEdge(V from, V to) {
        Preconditions.checkArgument(vertices.containsKey(from) && vertices.containsKey(to));
        adjacencyList.put(from, to);
        vertices.put(to, vertices.get(to) + 1);
    }

    /**
     * one-time use only, calling this multiple times on the same graph won't work
     */
    List<V> topologicalSort() {
        Preconditions.checkState(!wasSorted);
        wasSorted = true;

        Queue<V> queue = new LinkedList<V>();

        for (Map.Entry<V, Integer> entry : vertices.entrySet()) {
            if (entry.getValue() == 0)
                queue.add(entry.getKey());
        }

        List<V> result = Lists.newArrayList();
        while (!queue.isEmpty()) {
            V vertex = queue.remove();
            result.add(vertex);
            for (V successor : adjacencyList.get(vertex)) {
                if (decrementAndGetCount(successor) == 0)
                    queue.add(successor);
            }
        }

        if (result.size() != vertices.size())
            throw new DriverInternalError("failed to perform topological sort, graph has a cycle");

        return result;
    }

    private int decrementAndGetCount(V vertex) {
        Integer count = vertices.get(vertex);
        count = count - 1;
        vertices.put(vertex, count);
        return count;
    }
}
