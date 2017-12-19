package org.neo4j.graphalgo.impl.kmeans;

import com.carrotsearch.hppc.IntSet;

/**
 * @author mknblch
 */
public interface RootSelectionStrategy {

    IntSet roots(int[] cluster, int k);
}
