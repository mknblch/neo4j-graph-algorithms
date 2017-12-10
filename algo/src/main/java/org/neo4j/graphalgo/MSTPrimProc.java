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
import org.neo4j.graphalgo.core.utils.container.UndirectedTree;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.impl.MSTPrim;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class MSTPrimProc {

    public static final String CONFIG_WRITE_RELATIONSHIP = "writeProperty";
    public static final String CONFIG_WRITE_RELATIONSHIP_DEFAULT = "mst";

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure(value = "algo.mst", mode = Mode.WRITE)
    @Description("CALL algo.mst(label:String, relationshipType:String, weightProperty:String, startNodeId:long, {" +
            "writeProperty:String}) " +
            "YIELD loadMillis, computeMillis, writeMillis, weightSum, effectiveNodeCount")
    public Stream<MSTPrim.Result> mst(
            @Name(value = "label") String label,
            @Name(value = "relationshipType") String relationship,
            @Name(value = "weightProperty") String weightProperty,
            @Name(value = "startNodeId") long startNode,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        final MSTPrim.Builder builder = new MSTPrim.Builder();
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

        final MSTPrim mstPrim = new MSTPrim(graph)
                .withProgressLogger(ProgressLogger.wrap(log, "MST(Prim)"))
                .withTerminationFlag(TerminationFlag.wrap(transaction));

        builder.timeEval(() -> {
            mstPrim.compute(root);
        });

        builder.withWeightSum(mstPrim.getSumW())
                .withEffectiveNodeCount(mstPrim.getEffectiveNodeCount());

        if (configuration.isWriteFlag()) {
            final UndirectedTree minimumSpanningTree = mstPrim.getMinimumSpanningTree();
            mstPrim.release();
            builder.timeWrite(() -> {
                Exporter.of(graph, api)
                        .withLog(log)
                        .build()
                        .writeRelationships(
                                configuration.get(CONFIG_WRITE_RELATIONSHIP, CONFIG_WRITE_RELATIONSHIP_DEFAULT),
                                (ops, typeId) -> minimumSpanningTree.forEachBFS(root, writeBack((int) typeId, graph, ops))
                        );
            });
        }

        return Stream.of(builder.build());
    }

    private static RelationshipConsumer writeBack(int typeId, Graph mapping, DataWriteOperations ops) {
        return (source, target, rid) -> {
            try {
                ops.relationshipCreate(
                        typeId,
                        mapping.toOriginalNodeId(source),
                        mapping.toOriginalNodeId(target)
                );
            } catch (KernelException e) {
                throw Exceptions.launderedException(e);
            }
            return true;
        };
    }
}
