package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.cursors.IntCursor;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.queue.IntPriorityQueue;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * @author mknblch
 */
public class KMeans extends Algorithm<KMeans> {

    private final Graph graph;
    private final ExecutorService pool;
    private final int concurrency;

    private final int[] cluster;
    private final int[] distance;
    private final IntArrayList roots;
    private final int nodeCount;


    public KMeans(Graph graph, ExecutorService pool, int concurrency) {
        this.graph = graph;
        nodeCount = (int) graph.nodeCount();
        this.pool = pool;
        this.concurrency = concurrency;
        cluster = new int[nodeCount];
        depth = new int[nodeCount];
        Arrays.setAll(cluster, i -> i);
        roots = new IntArrayList();
    }

    public KMeans compute(int k) {
        distributeRandom(k);

        return this;
    }

    private void assignClusters() {

        final ArrayList<Runnable> runnables = new ArrayList<>();

        for (IntCursor root : roots) {
            runnables.add(() -> distance(root.value));
        }
        ParallelUtil.runWithConcurrency(concurrency, runnables, pool);

    }

    private void distributeRandom(int k) {
        for (int i = 0; i < k; i++) {
            roots.add((int) (Math.random() * nodeCount));
        }
    }

    public double[] distance(int startNode) {
        final IntPriorityQueue queue = IntPriorityQueue.min(nodeCount);
        final double[] distance = new double[nodeCount];
        Arrays.fill(distance, Double.POSITIVE_INFINITY);
        distance[startNode] = 0d;
        queue.add(startNode, 0d);
        while (!queue.isEmpty() && running()) {
            final int node = queue.pop();
            final double sourceDistance = distance[node];
            // scan relationships
            graph.forEachRelationship(
                    node,
                    Direction.OUTGOING,
                    (source, target, relId, weight) -> {
                        // relax
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

    @Override
    public KMeans me() {
        return this;
    }

    @Override
    public KMeans release() {
        return this;
    }
}
