package org.neo4j.graphalgo.impl.kmeans;

import org.neo4j.graphalgo.core.utils.AtomicDoubleArray;

/**
 * @author mknblch
 */
public class Distance {

    private final AtomicDoubleArray data;

    public Distance(int nodeCount) {
        data = new AtomicDoubleArray(nodeCount);

    }
}
