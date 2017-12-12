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
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.container.UndirectedTree;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphalgo.core.utils.queue.*;
import org.neo4j.graphalgo.core.utils.traverse.SimpleBitSet;
import org.neo4j.graphdb.Direction;

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

    private final Graph graph;
    private final int nodeCount;

    private LongPriorityQueue priorityQueue;
    private IntScatterSet roots;
    private UndirectedTree undirectedTree;

    public KSpanningTree(Graph graph) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());
    }

    public KSpanningTree compute(int startNode, int k, boolean max) {

        final LongPriorityQueue priorityQueue;
        final Prim prim = new Prim(graph);
        undirectedTree = new UndirectedTree(nodeCount);
        if (max) {
            prim.computeMaximumSpanningTree(startNode);
            priorityQueue = LongPriorityQueue.min(nodeCount);
        } else {
            prim.computeMinimumSpanningTree(startNode);
            priorityQueue = LongPriorityQueue.max(nodeCount);
        }

        prim.getSpanningTree().forEach((s, t, r) -> {
            System.out.print(s + " -> " + t + ", ");
            undirectedTree.addRelationship(s, t);
            priorityQueue.add(RawValues.combineIntInt(s, t), graph.weightOf(s, t));
            return true;
        });
        System.out.println();

        System.out.println(undirectedTree.toString(startNode));

        roots = new IntScatterSet(k);
        roots.add(startNode);
        for (int i = 0; i < k-1; i++) {
            final long transition = priorityQueue.pop();
            final int head = RawValues.getHead(transition);
            final int tail = RawValues.getTail(transition);
            roots.add(tail);
            System.out.println("cut " + head + " / " +  tail + " " +  priorityQueue.topCost());
            undirectedTree.removeRelationship(head, tail);
        }
        return this;
    }

    public IntScatterSet getRoots() {
        return roots;
    }

    public UndirectedTree getUndirectedTree() {
        return undirectedTree;
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
