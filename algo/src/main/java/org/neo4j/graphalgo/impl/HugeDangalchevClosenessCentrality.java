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

import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.PagedAtomicDoubleArray;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.impl.msbfs.BfsConsumer;
import org.neo4j.graphalgo.impl.msbfs.MultiSourceBFS;
import org.neo4j.graphdb.Direction;

import java.util.concurrent.ExecutorService;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Dangalchev Closeness Centrality
 *
 * hugification:
 *  - paged atomic double array : farness array
 *
 * @author mknblch
 */
public class HugeDangalchevClosenessCentrality extends Algorithm<HugeDangalchevClosenessCentrality> {

    private HugeGraph graph;
    private PagedAtomicDoubleArray farness;

    private final int concurrency;
    private final ExecutorService executorService;
    private final long nodeCount;

    public HugeDangalchevClosenessCentrality(HugeGraph graph, int concurrency, ExecutorService executorService, AllocationTracker tracker) {
        this.graph = graph;
        this.nodeCount = graph.nodeCount();
        this.concurrency = concurrency;
        this.executorService = executorService;
        farness = PagedAtomicDoubleArray.newArray(graph.nodeCount(), tracker);
    }

    public HugeDangalchevClosenessCentrality compute() {

        final ProgressLogger progressLogger = getProgressLogger();

        final BfsConsumer consumer = (nodeId, depth, sourceNodeIds) -> {
            int len = sourceNodeIds.size();
            farness.add(nodeId, len * 1.0 / Math.pow(2, depth));
            progressLogger.logProgress((double) nodeId / (nodeCount - 1));
        };

        new MultiSourceBFS(graph, graph, Direction.OUTGOING, consumer)
                .run(concurrency, executorService);

        return this;
    }

    public Stream<DangalchevClosenessCentrality.Result> resultStream() {
        return LongStream.range(0, nodeCount)
                .mapToObj(nodeId -> new DangalchevClosenessCentrality.Result(
                        graph.toOriginalNodeId(nodeId),
                        farness.get(nodeId)));
    }

    public void export(final String propertyName, final Exporter exporter) {
        exporter.write(
                propertyName,
                farness,
                (PropertyTranslator.OfDouble<PagedAtomicDoubleArray>) PagedAtomicDoubleArray::get);

    }

    @Override
    public HugeDangalchevClosenessCentrality me() {
        return this;
    }

    @Override
    public HugeDangalchevClosenessCentrality release() {
        graph = null;
        farness = null;
        return this;
    }

}
