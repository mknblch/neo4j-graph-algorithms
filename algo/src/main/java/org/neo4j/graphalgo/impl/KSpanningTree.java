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

import com.carrotsearch.hppc.*;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.api.RelationshipWeights;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.container.UndirectedTree;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphalgo.core.utils.queue.*;
import org.neo4j.graphalgo.core.utils.traverse.SimpleBitSet;
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
public class KSpanningTree extends Algorithm<KSpanningTree> {

    private final IdMapping idMapping;
    private final RelationshipIterator relationshipIterator;
    private final RelationshipWeights weights;

    private final int nodeCount;

    private DisjointSetStruct setStruct;


    public KSpanningTree(IdMapping idMapping, RelationshipIterator relationshipIterator, RelationshipWeights weights) {
        this.idMapping = idMapping;
        this.relationshipIterator = relationshipIterator;
        this.weights = weights;
        nodeCount = Math.toIntExact(idMapping.nodeCount());
    }

    private static void print(int[] parents) {
        for (int i = 0; i < parents.length; i++) {
            System.out.printf("%2d ", parents[i]);
        }
        System.out.println();
        for (int i = 0; i < parents.length; i++) {
            System.out.printf("%2d ", i);
        }
        System.out.println("\n");
    }


    public KSpanningTree compute(int startNode, int k, boolean max) {


        System.out.println(max ? "max" : "min");

        final IntPriorityQueue priorityQueue;
        if (max) {
            priorityQueue = IntPriorityQueue.min();
        } else {
            priorityQueue = IntPriorityQueue.max();
        }
        final int[] parents = prim(startNode, max, priorityQueue);

        print(parents);

        final PrimitiveIntIterator iterator = priorityQueue.iterator();
        for (;iterator.hasNext();) {
            final int cut = iterator.next();
            System.out.println("cutti " + cut);
        }

        // remove top k-1 relationships
        for (int i = 0; i < k - 1; i++) {
            final int cutNode = priorityQueue.pop();
            System.out.println("cutNode = " + cutNode);
            parents[cutNode] = -1;
        }

        setStruct = new DisjointSetStruct(nodeCount).reset();
        for (int i = 0; i < nodeCount; i++) {
            final int parent = parents[i];
            if (parent == -1) {
                continue;
            }
            setStruct.union(parent, i);
        }

        return this;
    }

    private int[] prim(int startNode, boolean max, IntPriorityQueue priorityQueue) {
        final int[] parent = new int[nodeCount];
        Arrays.fill(parent, -1);
        final IntDoubleMap cost = new IntDoubleScatterMap(nodeCount);
        final SharedIntPriorityQueue queue = SharedIntPriorityQueue.min(
                nodeCount,
                cost,
                Double.MAX_VALUE);
        final ProgressLogger logger = getProgressLogger();
        final SimpleBitSet visited = new SimpleBitSet(nodeCount);
        cost.put(startNode, 0.0);
        queue.add(startNode, -1.0);
        int effectiveNodeCount = 0;
        while (!queue.isEmpty() && running()) {
            final int node = queue.pop();
            if (visited.contains(node)) {
                continue;
            }
            effectiveNodeCount++;
            visited.put(node);
            relationshipIterator.forEachRelationship(node, Direction.OUTGOING, (s, t, r) -> {
                if (visited.contains(t)) {
                    return true;
                }
                // invert weight to calculate maximum
                final double w = weights.weightOf(s, t);
                final double v = max ? -w : w;
                if (v < cost.getOrDefault(t, Double.MAX_VALUE)) {
                    cost.put(t, v);
                    queue.add(t, -1.0);
                    parent[t] = s;
                    priorityQueue.add(Math.max(s, t), w);
                }
                return true;
            });
            logger.logProgress(nodeCount - 1, effectiveNodeCount);
        }
        logger.logDone(() -> "Prim done");
        return parent;
    }

    public DisjointSetStruct getSetStruct() {
        return setStruct;
    }

    @Override
    public KSpanningTree me() {
        return this;
    }

    @Override
    public KSpanningTree release() {
        return this;
    }

}
