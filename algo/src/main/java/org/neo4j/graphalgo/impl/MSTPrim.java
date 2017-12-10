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

import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.container.UndirectedTree;
import org.neo4j.graphalgo.core.utils.queue.LongMinPriorityQueue;
import org.neo4j.graphalgo.core.utils.traverse.SimpleBitSet;
import org.neo4j.graphalgo.results.AbstractResultBuilder;
import org.neo4j.graphdb.Direction;

import static org.neo4j.graphalgo.core.utils.RawValues.*;

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
public class MSTPrim extends Algorithm<MSTPrim> {

    private final Graph graph;
    private final int nodeCount;

    private UndirectedTree minimumSpanningTree;

    private double sumW;
    private double minW;
    private double maxW;

    private int effectiveNodeCount;

    public MSTPrim(Graph graph) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());
    }

    /**
     * compute the minimum weight spanning tree starting at node startNode
     */
    public MSTPrim compute(int startNode) {
        this.sumW = 0.0;
        this.maxW = 0.0;
        this.minW = Double.MAX_VALUE;
        this.effectiveNodeCount = 1;
        final ProgressLogger logger = getProgressLogger();
        final LongMinPriorityQueue queue = new LongMinPriorityQueue();
        final SimpleBitSet visited = new SimpleBitSet(nodeCount);
        minimumSpanningTree = new UndirectedTree(nodeCount);
        // initially add all relations from startNode to the priority queue
        visited.put(startNode);
        graph.forEachRelationship(startNode, Direction.OUTGOING, (s, t, r) -> {
            // encode relationship as long
            queue.add(combineIntInt(s, t), graph.weightOf(s, t));
            return true;
        });
        while (!queue.isEmpty() && running()) {
            // retrieve cheapest transition
            final long transition = queue.pop();
            final int tailId = getTail(transition);
            if (visited.contains(tailId)) {
                continue;
            }
            visited.put(tailId);
            final int headId = getHead(transition);
            minimumSpanningTree.addRelationship(headId, tailId);
            final double w = graph.weightOf(headId, tailId);
            this.sumW += w;
            this.minW = Math.min(minW, w);
            this.maxW = Math.max(maxW, w);
            effectiveNodeCount++;
            // add new candidates
            graph.forEachRelationship(tailId, Direction.OUTGOING, (s, t, r) -> {
                queue.add(combineIntInt(s, t), graph.weightOf(s, t));
                return true;
            });
            logger.logProgress(nodeCount - 1, effectiveNodeCount);
        }
        return this;
    }

    public UndirectedTree getMinimumSpanningTree() {
        return minimumSpanningTree;
    }

    @Override
    public MSTPrim me() {
        return this;
    }

    @Override
    public MSTPrim release() {
        minimumSpanningTree = null;
        return null;
    }

    public double getSumW() {
        return sumW;
    }

    public int getEffectiveNodeCount() {
        return effectiveNodeCount;
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
