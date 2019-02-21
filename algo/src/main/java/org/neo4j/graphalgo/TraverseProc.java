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
package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.*;
import org.neo4j.graphalgo.impl.Traversal;
import org.neo4j.graphalgo.impl.walking.WalkPath;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Path;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class TraverseProc {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

/*
    @Procedure(value = "algo.bfs", mode = Mode.WRITE)
    @Description("CALL algo.bfs.stream(label:String, relationshipType:String, startNodeId:long, {writeProperty:String, target:-1, maxDepth:2147483647}) " +
            "YIELD NodeId")
    public Stream<Path> bfs(
            @Name(value = "label") String label,
            @Name(value = "relationshipType") String relationship,
            @Name(value = "startNodeId") long startNode,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {


        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        final Graph graph;
        graph = new GraphLoader(api, Pools.DEFAULT)
                .withOptionalLabel(label)
                .withOptionalRelationshipType(relationship)
                .withoutNodeWeights()
                .asUndirected(true)
                .withLog(log)
                .load(configuration.getGraphImpl(HugeGraph.TYPE));

        final int source = graph.toMappedNodeId(startNode);
        final Traversal mstPrim = new Traversal(graph)
                .withProgressLogger(ProgressLogger.wrap(log, "BFS"))
                .withTerminationFlag(TerminationFlag.wrap(transaction));
        final long targetNode = configuration.getNumber("target", -1L).longValue();
        final int mappedTargetNode = targetNode == -1L ? -1 : graph.toMappedNodeId(targetNode);
        final long[] nodes = mstPrim.computeBfs(
                source,
                configuration.getDirection(Direction.OUTGOING),
                mappedTargetNode,
                configuration.getNumber("maxDepth", Integer.MAX_VALUE).intValue());

        return Stream.of(WalkPath.toPath(api, nodes));
    }

    @Procedure(value = "algo.dfs", mode = Mode.WRITE)
    @Description("CALL algo.dfs.stream(label:String, relationshipType:String, startNodeId:long, {writeProperty:String, target:-1, maxDepth:2147483647}) " +
            "YIELD NodeId")
    public Stream<Path> dfs(
            @Name(value = "label") String label,
            @Name(value = "relationshipType") String relationship,
            @Name(value = "startNodeId") long startNode,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        final Graph graph;
        graph = new GraphLoader(api, Pools.DEFAULT)
                .withOptionalLabel(label)
                .withOptionalRelationshipType(relationship)
                .withoutNodeWeights()
                .asUndirected(true)
                .withLog(log)
                .load(configuration.getGraphImpl(HugeGraph.TYPE));

        final int source = graph.toMappedNodeId(startNode);
        final Traversal mstPrim = new Traversal(graph)
                .withProgressLogger(ProgressLogger.wrap(log, "BFS"))
                .withTerminationFlag(TerminationFlag.wrap(transaction));
        final long targetNode = configuration.getNumber("target", -1L).longValue();
        final int mappedTargetNode = targetNode == -1L ? -1 : graph.toMappedNodeId(targetNode);
        final long[] nodes = mstPrim.computeDfs(
                source,
                configuration.getDirection(Direction.OUTGOING),
                mappedTargetNode,
                configuration.getNumber("maxDepth", Integer.MAX_VALUE).intValue());

        return Stream.of(WalkPath.toPath(api, nodes));
    }
*/
}
