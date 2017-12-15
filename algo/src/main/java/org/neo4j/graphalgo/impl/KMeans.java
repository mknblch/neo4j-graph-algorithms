package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.IntArrayList;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.impl.msbfs.BfsConsumer;
import org.neo4j.graphalgo.impl.msbfs.BfsSources;
import org.neo4j.graphalgo.impl.msbfs.MultiSourceBFS;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;

/**
 * @author mknblch
 */
public class KMeans extends Algorithm<KMeans> {

    private final Graph graph;

    private final int[] cluster;
    private final int[] depth;
    private final IntArrayList roots;
    private final int nodeCount;

    public KMeans(Graph graph) {
        this.graph = graph;
        nodeCount = (int) graph.nodeCount();
        cluster = new int[nodeCount];
        depth = new int[nodeCount];
        Arrays.setAll(cluster, i -> i);
        roots = new IntArrayList();
    }

    public KMeans compute(int k) {
        assignRandom(k);
        new MultiSourceBFS(graph, graph, Direction.OUTGOING, (nodeId, depth, sourceNodeIds) -> {

        }, roots.toArray());
        return this;
    }

    private void assignRandom(int k) {
        for (int i = 0; i < k; i++) {
            roots.add((int) (Math.random() * nodeCount));
        }
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
