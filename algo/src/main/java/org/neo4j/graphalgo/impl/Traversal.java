/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 * <p>
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 * <p>
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.DoubleArrayDeque;
import com.carrotsearch.hppc.IntArrayDeque;
import com.carrotsearch.hppc.LongArrayList;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.Direction;

import java.util.function.ObjDoubleConsumer;
import java.util.function.ObjIntConsumer;

/**
 * @author mknblch
 */
public class Traversal extends Algorithm<Traversal> {

    private final int nodeCount;
    private Graph graph;
    private IntArrayDeque nodes;
    private IntArrayDeque sources;
    private DoubleArrayDeque weights;
    private BitSet visited;

    public Traversal(Graph graph) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());
        nodes = new IntArrayDeque(nodeCount);
        sources = new IntArrayDeque(nodeCount);
        weights = new DoubleArrayDeque(nodeCount);
        visited = new BitSet(nodeCount);
    }

    public long[] computeBfs(long sourceId, Direction direction, Predicate exitCondition) {
        return traverse(graph.toMappedNodeId(sourceId), direction, exitCondition, (s, t, w) -> 0., IntArrayDeque::addLast, DoubleArrayDeque::addLast);
    }

    public long[] computeDfs(long sourceId, Direction direction, Predicate exitCondition) {
        return traverse(graph.toMappedNodeId(sourceId), direction, exitCondition, (s, t, w) -> 0., IntArrayDeque::addFirst, DoubleArrayDeque::addLast);
    }

    public long[] computeBfs(long sourceId, Direction direction, Predicate exitCondition, Aggregator aggregator) {
        return traverse(graph.toMappedNodeId(sourceId), direction, exitCondition, aggregator, IntArrayDeque::addLast, DoubleArrayDeque::addLast);
    }

    public long[] computeDfs(long sourceId, Direction direction, Predicate exitCondition, Aggregator aggregator) {
        return traverse(graph.toMappedNodeId(sourceId), direction, exitCondition, aggregator, IntArrayDeque::addFirst, DoubleArrayDeque::addLast);
    }

    /**
     * calc path
     *
     * @return true if a path has been found, false otherwise
     */
    private long[] traverse(int sourceNode,
                            Direction direction,
                            Predicate exitCondition,
                            Aggregator agg,
                            ObjIntConsumer<IntArrayDeque> nodeFunc,
                            ObjDoubleConsumer<DoubleArrayDeque> weightFunc) {

        final LongArrayList list = new LongArrayList(nodeCount);
        nodes.clear();
        sources.clear();
        visited.clear();
        nodeFunc.accept(nodes, sourceNode);
        nodeFunc.accept(sources, sourceNode);
        weightFunc.accept(weights, 0.);
        visited.set(sourceNode);
        loop:
        while (!nodes.isEmpty() && running()) {
            final int source = sources.removeFirst();
            final int node = nodes.removeFirst();
            final double weight = weights.removeFirst();
            switch (exitCondition.test(source, node, weight)) {
                case BREAK:
                    list.add(graph.toOriginalNodeId(node));
                    break loop;
                case CONTINUE:
                    continue loop;
                case FOLLOW:
                    list.add(graph.toOriginalNodeId(node));
                    break;
            }
            graph.forEachRelationship(
                    node,
                    direction, (s, t, relId) -> {
                        if (!visited.get(t)) {
                            visited.set(t);
                            nodeFunc.accept(sources, node);
                            nodeFunc.accept(nodes, t);
                            weightFunc.accept(weights, agg.apply(s, t, weight));
                        }
                        return running();
                    });
        }
        return list.toArray();
    }

    @Override
    public Traversal me() {
        return this;
    }

    @Override
    public Traversal release() {
        nodes = null;
        weights = null;
        visited = null;
        return this;
    }

    public interface Predicate {

        enum Result {
            FOLLOW, BREAK, CONTINUE
        }

        Result test(int sourceNode, int currentNode, double weightAtSource);
    }

    public interface Aggregator {

        double apply(int sourceNode, int currentNode, double weightAtSource);
    }
}
