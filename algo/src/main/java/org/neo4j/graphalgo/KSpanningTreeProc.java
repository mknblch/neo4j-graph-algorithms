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
package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.huge.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphalgo.core.utils.paged.HugeDisjointSetStruct;
import org.neo4j.graphalgo.core.write.DisjointSetStructTranslator;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.HugeDisjointSetStructTranslator;
import org.neo4j.graphalgo.impl.DSSResult;
import org.neo4j.graphalgo.impl.KSpanningTree;
import org.neo4j.graphalgo.impl.Prim;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class KSpanningTreeProc {

    private static final String CONFIG_CLUSTER_PROPERTY = "partitionProperty";
    private static final String DEFAULT_CLUSTER_PROPERTY = "partition";

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;


    @Procedure(value = "algo.spanningTree.kmax", mode = Mode.WRITE)
    @Description("CALL algo.spanningTree.kmax(label:String, relationshipType:String, weightProperty:String, startNodeId:long, k:int, {" +
            "writeProperty:String}) " +
            "YIELD loadMillis, computeMillis, writeMillis, effectiveNodeCount")
    public Stream<Prim.Result> kmax(
            @Name(value = "label") String label,
            @Name(value = "relationshipType") String relationship,
            @Name(value = "weightProperty") String weightProperty,
            @Name(value = "startNodeId") long startNode,
            @Name(value = "k") int k,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return spanningTree(label, relationship, weightProperty, startNode, k, config, true);
    }

    @Procedure(value = "algo.spanningTree.kmin", mode = Mode.WRITE)
    @Description("CALL algo.spanningTree.kmin(label:String, relationshipType:String, weightProperty:String, startNodeId:long, k:int, {" +
            "writeProperty:String}) " +
            "YIELD loadMillis, computeMillis, writeMillis, effectiveNodeCount")
    public Stream<Prim.Result> kmin(
            @Name(value = "label") String label,
            @Name(value = "relationshipType") String relationship,
            @Name(value = "weightProperty") String weightProperty,
            @Name(value = "startNodeId") long startNode,
            @Name(value = "k") int k,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return spanningTree(label, relationship, weightProperty, startNode, k, config, true);
    }

    public Stream<Prim.Result> spanningTree(String label,
                                            String relationship,
                                            String weightProperty,
                                            long startNode,
                                            int k,
                                            Map<String, Object> config,
                                            boolean max) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        final Prim.Builder builder = new Prim.Builder();
        final Graph graph;
        try (ProgressTimer timer = builder.timeLoad()) {
            graph = new GraphLoader(api, Pools.DEFAULT)
                    .withOptionalLabel(label)
                    .withOptionalRelationshipType(relationship)
                    .withRelationshipWeightsFromProperty(weightProperty, configuration.getWeightPropertyDefaultValue(Double.MAX_VALUE))
                    .withoutNodeWeights()
                    .asUndirected(true)
                    .withLog(log)
                    .load(configuration.getGraphImpl(HugeGraphFactory.class));
        }
        final int root = graph.toMappedNodeId(startNode);

        final KSpanningTree kSpanningTree = new KSpanningTree(graph, graph, graph);

        builder.timeEval(() -> {
            kSpanningTree.compute(root, k, max);
        });

        try (ProgressTimer timer = builder.timeWrite()) {
            final DisjointSetStruct struct = kSpanningTree.getSetStruct();

            final Exporter exporter = Exporter.of(api, graph)
                    .withLog(log)
                    .parallel(
                            Pools.DEFAULT,
                            configuration.getConcurrency(),
                            TerminationFlag.wrap(transaction))
                    .build();

            exporter.write(
                    configuration.get(
                            CONFIG_CLUSTER_PROPERTY,
                            DEFAULT_CLUSTER_PROPERTY),
                    struct,
                    DisjointSetStructTranslator.INSTANCE);
        }

        return Stream.of(builder.build());
    }
}
