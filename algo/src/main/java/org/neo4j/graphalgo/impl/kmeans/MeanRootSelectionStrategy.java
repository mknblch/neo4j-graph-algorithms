package org.neo4j.graphalgo.impl.kmeans;

import com.carrotsearch.hppc.IntIntMap;
import com.carrotsearch.hppc.IntIntScatterMap;
import com.carrotsearch.hppc.IntSet;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.queue.IntPriorityQueue;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;

/**
 * @author mknblch
 */
public class MeanRootSelectionStrategy implements RootSelectionStrategy {

    private final Graph graph;

    private final ExecutorService pool;

    private final int concurrency;

    private final int nodeCount;

    public MeanRootSelectionStrategy(Graph graph, ExecutorService pool, int concurrency) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.pool = pool;
        this.concurrency = concurrency;
    }

    @Override
    public IntSet roots(int[] cluster, int k) {

        final IntIntScatterMap map = new IntIntScatterMap();
        for (int i = 0; i < cluster.length; i++) {
            final int v = map.getOrDefault(cluster[i], -1);
            if (v != -1) {
                continue;
            }
            map.put(cluster[i], i);
        }

        map.values()


        return null;
    }

    private double[] sumShortestPaths(int[] cluster, int startNode) {
        final int c = cluster[startNode];
        final IntPriorityQueue queue = IntPriorityQueue.min(nodeCount);
        final double[] distance = new double[nodeCount];
        Arrays.fill(distance, Double.POSITIVE_INFINITY);
        distance[startNode] = 0d;
        queue.add(startNode, 0d);
        while (!queue.isEmpty()) {
            final int node = queue.pop();
            final double sourceDistance = distance[node];
            // scan relationships
            graph.forEachRelationship(
                    node,
                    Direction.OUTGOING,
                    (source, target, relId, weight) -> {
                        // relax
                        if (cluster[target] != c) {
                            return true;
                        }
                        final double targetDistance = weight + sourceDistance;
                        if (targetDistance < distance[target]) {
                            distance[target] = targetDistance;
                            queue.add(target, targetDistance);
                        }
                        return true;
                    });
        }

        return distance;
    }
}
