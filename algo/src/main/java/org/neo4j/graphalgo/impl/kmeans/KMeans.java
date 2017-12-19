package org.neo4j.graphalgo.impl.kmeans;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.predicates.IntPredicate;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.AtomicDoubleArray;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.queue.IntPriorityQueue;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;

/**
 * @author mknblch
 */
public class KMeans extends Algorithm<KMeans> {

    private static final RootSelectionStrategy RANDOM = new RandomRootSelectionStrategy();

    private final Graph graph;
    private final AllocationTracker tracker;
    private final ExecutorService pool;
    private final int concurrency;

    private final int[] cluster;
    private final AtomicDoubleArray distance;
    private final int nodeCount;

    private IntSet roots;

    private int maxIterations = 10;
    private RootSelectionStrategy rootSelectionStrategy;
    private boolean convergence = false;

    public KMeans(Graph graph, AllocationTracker tracker, ExecutorService pool, int concurrency) {
        this.graph = graph;
        nodeCount = (int) graph.nodeCount();
        this.tracker = tracker;
        this.pool = pool;
        this.concurrency = concurrency;
        cluster = new int[nodeCount];
        distance = new AtomicDoubleArray(nodeCount);
        Arrays.setAll(cluster, i -> i);
    }

    public KMeans compute(int k) {
        roots = RANDOM.roots(cluster, k);
        for (int i = 0; i < maxIterations; i++) {
            if (!cluster()) {
                this.convergence = true;
                return this;
            }
            final IntSet newRoots = rootSelectionStrategy.roots(cluster, k);

        }
        return this;
    }

    private static boolean difference(IntSet a, IntSet b) {

    }

    private boolean cluster() {
        final boolean[] changes = {false};
        final ArrayList<Runnable> runnables = new ArrayList<>();
        for (IntCursor root : roots) {
            runnables.add(() -> changes[0] |= assignNearestTo(root.value));
        }
        ParallelUtil.runWithConcurrency(concurrency, runnables, pool);
        return changes[0];
    }

    private boolean assignNearestTo(int startNode) {
        final boolean[] changes = {false};
        tracker.add((4 + 8) * nodeCount);
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
                            if (this.distance.trySetMin(target, targetDistance)) {
                                cluster[target] = startNode;
                                changes[0] = true;
                            }
                        }
                        return true;
                    });
        }
        tracker.remove((4 + 8) * nodeCount);
        return changes[0];
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
