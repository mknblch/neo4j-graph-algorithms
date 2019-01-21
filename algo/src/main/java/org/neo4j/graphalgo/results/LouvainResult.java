/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.results;

/**
 * @author mknblch
 */
public class LouvainResult extends CommunityResult {

    private LouvainResult(long loadMillis, long computeMillis, long writeMillis, long nodes, long communityCount, long iterations, boolean convergence, long p99, long p95, long p90, long p75, long p50, int[] biggestCommunities) {
        super(loadMillis, computeMillis, writeMillis, postProcessingMillis, nodes, communityCount, iterations, convergence, p99, p95, p90, p75, p50, p25, p10, p05, p01, biggestCommunities);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends AbstractCommunityResultBuilder<LouvainResult> {

        @Override
        public LouvainResult build() {
            return new LouvainResult(
                    loadDuration,
                    evalDuration,
                    writeDuration,
                    nodes,
                    communityCount == -1 ? communityMap.size() : communityCount,
                    iterations, convergence,
                    histogram.getValueAtPercentile(.99),
                    histogram.getValueAtPercentile(.95),
                    histogram.getValueAtPercentile(.90),
                    histogram.getValueAtPercentile(.75),
                    histogram.getValueAtPercentile(.50),
                    getTopN(3));
        }
    }

}
