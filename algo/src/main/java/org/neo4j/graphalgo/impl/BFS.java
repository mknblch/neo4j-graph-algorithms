package org.neo4j.graphalgo.impl;

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

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.IntArrayDeque;
import com.carrotsearch.hppc.IntStack;
import com.carrotsearch.hppc.LongArrayList;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.impl.walking.WalkPath;
import org.neo4j.graphalgo.impl.walking.WalkResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

/**
 * @author mknblch
 */
public class BFS extends Algorithm<BFS> {

    private final int nodeCount;
    private final GraphDatabaseAPI api;
    private Graph graph;
    private IntArrayDeque nodes;
    private IntArrayDeque depths;
    private BitSet visited;

    public BFS(GraphDatabaseAPI api, Graph graph) {
        this.api = api;
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());
        nodes = new IntArrayDeque(nodeCount);
        depths = new IntArrayDeque(nodeCount);
        visited = new BitSet(nodeCount);
    }

    public WalkResult computeToTarget(int source, Direction direction, int target) {
        return toWalkResult(bfs(source, direction, target, Integer.MAX_VALUE, Integer.MAX_VALUE));
    }

    public WalkResult computeMaxDepth(int source, Direction direction, int maxDepth) {
        return toWalkResult(bfs(source, direction, -1, maxDepth, Integer.MAX_VALUE));
    }

    public WalkResult computeMaxVisits(int source, Direction direction, int maxVisits) {
        return toWalkResult(bfs(source, direction, -1, Integer.MAX_VALUE, maxVisits));
    }

    private WalkResult toWalkResult(long[] nodes) {
        try (Transaction transaction = api.beginTx()) {
            final Path path = WalkPath.toPath(api, nodes);
            final WalkResult result = new WalkResult(nodes, path);
            transaction.success();
            return result;
        }
    }

    /**
     * calc path
     *
     * @return true if a path has been found, false otherwise
     */
    private long[] bfs(int source, Direction direction, int target, int maxDepth, int maxVisits) {

        final LongArrayList list = new LongArrayList(nodeCount);
        final int[] visits = {0};

        nodes.clear();
        depths.clear();
        visited.clear();
        nodes.addLast(source);
        depths.addLast(0);
        visited.set(source);

        while (!nodes.isEmpty() && running()) {
            visits[0]++;
            final int node = nodes.removeFirst();
            final int depth = depths.removeFirst();
            list.add(graph.toOriginalNodeId(node));
            if (target == node) {
                break;
            }
            if (visits[0] >= maxVisits) {
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
                            nodes.addLast(t);
                            depths.addLast(depth + 1);
                        }
                        return running();
                    });
        }
        return list.toArray();
    }


    @Override
    public BFS me() {
        return this;
    }

    @Override
    public BFS release() {
        nodes = null;
        depths = null;
        visited = null;
        return this;
    }
}
