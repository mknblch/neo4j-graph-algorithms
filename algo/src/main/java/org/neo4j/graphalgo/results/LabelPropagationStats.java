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
package org.neo4j.graphalgo.results;

public class LabelPropagationStats extends CommunityResult {

    public final long iterations;
    public final long nodes;
    public final boolean write, didConverge;
    public final String weightProperty, partitionProperty;

    public LabelPropagationStats(long loadMillis, long computeMillis, long writeMillis, long postProcessingMillis, long nodes, long communityCount, long p99, long p95, long p90, long p75, long p50, long p25, long p10, long p05, long p01, Long[] biggestCommunities, long iterations, boolean write, boolean didConverge, String weightProperty, String partitionProperty) {
        super(loadMillis, computeMillis, writeMillis, postProcessingMillis, nodes, communityCount, p99, p95, p90, p75, p50, p25, p10, p05, p01, biggestCommunities);
        this.iterations = iterations;
        this.write = write;
        this.didConverge = didConverge;
        this.weightProperty = weightProperty;
        this.partitionProperty = partitionProperty;
        this.nodes = nodes;
    }

    public static class Builder extends CommunityResultBuilder<LabelPropagationStats> {

        private long iterations = 0;
        private boolean didConverge = false;
        private boolean write;
        private String weightProperty;
        private String partitionProperty;

        public Builder iterations(final long iterations) {
            this.iterations = iterations;
            return this;
        }

        public Builder didConverge(final boolean didConverge) {
            this.didConverge = didConverge;
            return this;
        }

        public Builder write(final boolean write) {
            this.write = write;
            return this;
        }

        public Builder weightProperty(final String weightProperty) {
            this.weightProperty = weightProperty;
            return this;
        }

        public Builder partitionProperty(final String partitionProperty) {
            this.partitionProperty = partitionProperty;
            return this;
        }

        @Override
        protected LabelPropagationStats build(long loadMillis,
                                              long computeMillis,
                                              long writeMillis,
                                              long postProcessingMillis,
                                              long nodeCount,
                                              long communityCount,
                                              long p99,
                                              long p95,
                                              long p90,
                                              long p75,
                                              long p50,
                                              long p25,
                                              long p10,
                                              long p05,
                                              long p01,
                                              Long[] top3Communities) {
            return new LabelPropagationStats(
                    loadMillis,
                    computeMillis,
                    writeMillis,
                    postProcessingMillis,
                    nodeCount,
                    communityCount,
                    p99,
                    p95,
                    p90,
                    p75,
                    p50,
                    p25,
                    p10,
                    p05,
                    p01,
                    top3Communities,
                    iterations,
                    write,
                    didConverge,
                    weightProperty,
                    partitionProperty
            );
        }

        public LabelPropagationStats emptyResult() {
            return new LabelPropagationStats(
                    loadDuration,
                    evalDuration,
                    writeDuration,
                    -1,
                    0,
                    0,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    new Long[]{-1L, -1L, -1L},
                    iterations,
                    write,
                    didConverge,
                    weightProperty,
                    partitionProperty);
        }
    }
}
