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

import com.carrotsearch.hppc.IntScatterSet;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.cursors.IntCursor;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.container.UndirectedTree;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphalgo.core.utils.queue.LongMaxPriorityQueue;
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
public class KMeans extends Algorithm<KMeans> {

    private Graph graph;
    private DisjointSetStruct setStruct;

    private final int nodeCount;
    private final int k;

    public KMeans(Graph graph, int k) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());
        setStruct = new DisjointSetStruct(nodeCount);
        this.k = k;
    }

    /**
     * compute the minimum weight spanning tree starting at node startNode
     */
    public KMeans compute(int startNode) {
        final ProgressLogger logger = getProgressLogger();
        final LongMaxPriorityQueue queue = new LongMaxPriorityQueue();
        final SimpleBitSet visited = new SimpleBitSet(nodeCount);
        final UndirectedTree minimumSpanningTree = new UndirectedTree(nodeCount);
        final LongMinPriorityQueue maxQueue = new LongMinPriorityQueue(nodeCount);
        // initially add all relations from startNode to the priority queue
        visited.put(startNode);
        graph.forEachRelationship(startNode, Direction.BOTH, (s, t, r) -> {
            // encode relationship as long
            System.out.println("add " +  s  + " -> " + t  + " :" + graph.weightOf(s, t));
            queue.add(combineIntInt(s, t), graph.weightOf(s, t));
            return true;
        });
        int effectiveNodeCount = 1;
        while (!queue.isEmpty() && running()) {
            // retrieve cheapest transition
            final long transition = queue.pop();
            final int tailId = getTail(transition);
            if (visited.contains(tailId)) {
                continue;
            }
            visited.put(tailId);
            final int headId = getHead(transition);
            final double w = graph.weightOf(headId, tailId);
            minimumSpanningTree.addRelationship(headId, tailId);
            maxQueue.add(transition, w);
            System.out.println(" mst " + headId + " -> " + tailId + " : " + w);
            effectiveNodeCount++;
            // add new candidates
            graph.forEachRelationship(tailId, Direction.BOTH, (s, t, r) -> {
                System.out.println("add " +  s  + " -> " + t  + " :" + graph.weightOf(s, t));
                queue.add(combineIntInt(s, t), graph.weightOf(s, t));
                return true;
            });
            logger.logProgress(nodeCount - 1, effectiveNodeCount, () -> "mst calculation");
        }
        logger.logProgress(0, () -> "clustering");
        final IntSet roots = new IntScatterSet();
        for (int i = 0; i < k; i++) {
            final long transition = maxQueue.pop();
            final int head = getHead(transition);
            final int tail = getTail(transition);
            System.out.println("split at " + head + " / " + tail);
            minimumSpanningTree.removeRelationship(head, tail);
            roots.add(head);
            roots.add(tail);
        }
        setStruct.reset();
        for (IntCursor root : roots) {
            System.out.println("root = " + root.value);
            minimumSpanningTree.forEachDFS(root.value, (s, t, r) -> {
                System.out.println("join " + s +  " -> " + t);
                setStruct.union(s, t);
                return true;
            });
        }
        return this;
    }

    public DisjointSetStruct getSetStruct() {
        return setStruct;
    }

    @Override
    public KMeans me() {
        return this;
    }

    @Override
    public KMeans release() {
        graph = null;
        setStruct = null;
        return this;
    }

    public static class Result {

        public final long loadMillis;
        public final long computeMillis;
        public final long writeMillis;

        public Result(long loadMillis,
                      long computeMillis,
                      long writeMillis,
                      double weightSum,
                      double weightMin, double weightMax, int effectiveNodeCount) {
            this.loadMillis = loadMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
        }
    }

    public static class Builder extends AbstractResultBuilder<Result> {

        public Result build() {
            return null;
        }
    }

}
