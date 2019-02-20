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
import com.carrotsearch.hppc.IntArrayDeque;
import com.carrotsearch.hppc.LongArrayList;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.Direction;

import java.util.function.ObjIntConsumer;

/**
 * @author mknblch
 */
public class Traversal extends Algorithm<Traversal> {

    private final int nodeCount;
    private Graph graph;
    private IntArrayDeque nodes;
    private IntArrayDeque depths;
    private BitSet visited;

    public Traversal(Graph graph) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());
        nodes = new IntArrayDeque(nodeCount);
        depths = new IntArrayDeque(nodeCount);
        visited = new BitSet(nodeCount);
    }

    public long[] computeBfs(int source, Direction direction, int target, int maxDepth) {
        return traverse(source, direction, target, maxDepth, IntArrayDeque::addLast);
    }

    public long[] computeDfs(int source, Direction direction, int target, int maxDepth) {
        return traverse(source, direction, target, maxDepth, IntArrayDeque::addFirst);
    }

    /**
     * calc path
     *
     * @return true if a path has been found, false otherwise
     */
    private long[] traverse(int source, Direction direction, int target, int maxDepth, ObjIntConsumer<IntArrayDeque> pusher) {
        final LongArrayList list = new LongArrayList(nodeCount);
        nodes.clear();
        depths.clear();
        visited.clear();
        pusher.accept(nodes, source);
        pusher.accept(depths, 0);
        visited.set(source);
        while (!nodes.isEmpty() && running()) {
            final int node = nodes.removeFirst();
            final int depth = depths.removeFirst();
            list.add(graph.toOriginalNodeId(node));
            if (target == node) {
                break;
            }
            if (depth >= maxDepth) {
                break;
            }
            graph.forEachRelationship(
                    node,
                    direction, (s, t, relId) -> {
                        if (!visited.get(t)) {
                            visited.set(t);
                            pusher.accept(nodes, t);
                            pusher.accept(depths, depth + 1);
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
        depths = null;
        visited = null;
        return this;
    }
}
