package org.neo4j.graphalgo.impl.kmeans;

import com.carrotsearch.hppc.IntScatterSet;
import com.carrotsearch.hppc.IntSet;

/**
 * @author mknblch
 */
public class RandomRootSelectionStrategy implements RootSelectionStrategy {

    @Override
    public IntSet roots(int[] cluster, int k) {
        final IntScatterSet roots = new IntScatterSet(k);
        final int nodeCount = cluster.length;
        for (int i = 0; i < k; i++) {
            roots.add((int) (Math.random() * nodeCount));
        }
        return roots;
    }
}
