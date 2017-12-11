/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.IntDoubleMap;
import com.carrotsearch.hppc.IntDoubleScatterMap;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.container.UndirectedTree;
import org.neo4j.graphalgo.core.utils.queue.SharedIntPriorityQueue;
import org.neo4j.graphalgo.core.utils.traverse.SimpleBitSet;
import org.neo4j.graphalgo.results.AbstractResultBuilder;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;

/**
 * Sequential Single-Source minimum weight spanning tree algorithm (PRIM).
 * <p>
 * The algorithm computes the MST by traversing all nodes from a given
 * startNodeId. It aggregates all transitions into a MinPriorityQueue
 * and visits each (unvisited) connected node by following only the
 * cheapest transition and adding it to a specialized form of {@link UndirectedTree}.
 * <p>
 * The algorithm also computes the minimum, maximum and sum of all
 * weights in the MST.
 *
 * @author mknblch
 */
public class Prim extends Algorithm<Prim> {

    private final Graph graph;
    private final int nodeCount;
    private int effectiveNodeCount;
    private SpanningTree spanningTree;

    public Prim(Graph graph) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());

    }

    /**
     * compute the minimum weight spanning tree starting at node startNode
     */
    public Prim compute(int startNode) {
        return computeMinimumSpanningTree(startNode);
    }

    public Prim computeMaximumSpanningTree(int startNode) {

        spanningTree = new SpanningTree(nodeCount);

        final SharedIntPriorityQueue queue = new SharedIntPriorityQueue.Max(
                nodeCount,
                spanningTree.cost,
                Double.MAX_VALUE);

        this.effectiveNodeCount = 1;
        final ProgressLogger logger = getProgressLogger();
        final SimpleBitSet visited = new SimpleBitSet(nodeCount);
        // initially add all relations from startNode to the priority queue
        visited.put(startNode);
        queue.add(startNode, 0.0);
        spanningTree.cost.put(startNode, 0.0);
        while (!queue.isEmpty() && running()) {
            // retrieve cheapest transition
            final int node = queue.pop();
            visited.put(node);
            effectiveNodeCount++;
            // add new candidates
            graph.forEachRelationship(node, Direction.OUTGOING, (s, t, r) -> {
                if (visited.contains(t)) {
                    return true;
                }
                final double w = graph.weightOf(s, t);
                if (w > spanningTree.cost.getOrDefault(t, Double.MAX_VALUE)) {
                    spanningTree.cost.put(t, w);
                    queue.add(t, w);
                    spanningTree.parent[t] = s;
                }
                return true;
            });
            logger.logProgress(nodeCount - 1, effectiveNodeCount);
        }
        return this;
    }

    public Prim computeMinimumSpanningTree(int startNode) {

        spanningTree = new SpanningTree(nodeCount);

        final SharedIntPriorityQueue queue = new SharedIntPriorityQueue.Min(
                nodeCount,
                spanningTree.cost,
                Double.MAX_VALUE);

        this.effectiveNodeCount = 1;
        final ProgressLogger logger = getProgressLogger();
        final SimpleBitSet visited = new SimpleBitSet(nodeCount);
        // initially add all relations from startNode to the priority queue
        visited.put(startNode);
        queue.add(startNode, 0.0);
        spanningTree.cost.put(startNode, 0.0);
        while (!queue.isEmpty() && running()) {
            // retrieve cheapest transition
            final int node = queue.pop();
            visited.put(node);
            effectiveNodeCount++;
            // add new candidates
            graph.forEachRelationship(node, Direction.OUTGOING, (s, t, r) -> {
                if (visited.contains(t)) {
                    return true;
                }
                final double w = graph.weightOf(s, t);
                if (w < spanningTree.cost.getOrDefault(t, Double.MAX_VALUE)) {
                    spanningTree.cost.put(t, w);
                    queue.add(t, w);
                    spanningTree.parent[t] = s;
                }
                return true;
            });
            logger.logProgress(nodeCount - 1, effectiveNodeCount);
        }
        return this;
    }

    @Override
    public Prim me() {
        return this;
    }

    @Override
    public Prim release() {
        spanningTree = null;
        return this;
    }

    public int getEffectiveNodeCount() {
        return effectiveNodeCount;
    }

    public static class SpanningTree {

        public final int[] parent;
        public final IntDoubleMap cost;

        public SpanningTree(int nodeCount) {
            parent = new int[nodeCount];
            Arrays.fill(parent, -1);
            cost = new IntDoubleScatterMap();
        }

        public SpanningTree(int[] parent, IntDoubleMap cost) {
            this.parent = parent;
            this.cost = cost;
        }
    }

    public static class Result {

        public final long loadMillis;
        public final long computeMillis;
        public final long writeMillis;
        public final double weightSum;
        public final double weightMin;
        public final double weightMax;
        public final long effectiveNodeCount;

        public Result(long loadMillis,
                      long computeMillis,
                      long writeMillis,
                      double weightSum,
                      double weightMin, double weightMax, int effectiveNodeCount) {
            this.loadMillis = loadMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.weightSum = weightSum;
            this.weightMin = weightMin;
            this.weightMax = weightMax;
            this.effectiveNodeCount = effectiveNodeCount;
        }
    }

    public static class Builder extends AbstractResultBuilder<Result> {

        protected double weightSum = 0.0;
        protected double weightMin = 0.0;
        protected double weightMax = 0.0;
        protected int effectiveNodeCount = 0;

        public Builder withWeightSum(double weightSum) {
            this.weightSum = weightSum;
            return this;
        }

        public Builder withWeightMin(double weightMin) {
            this.weightMin = weightMin;
            return this;
        }

        public Builder withWeightMax(double weightMax) {
            this.weightMax = weightMax;
            return this;
        }

        public Builder withEffectiveNodeCount(int effectiveNodeCount) {
            this.effectiveNodeCount = effectiveNodeCount;
            return this;
        }

        public Result build() {
            return new Result(loadDuration,
                    evalDuration,
                    writeDuration,
                    weightSum,
                    weightMin, weightMax, effectiveNodeCount);
        }
    }

}
