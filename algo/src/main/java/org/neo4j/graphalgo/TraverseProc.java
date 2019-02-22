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


    @Procedure(value = "algo.bfs.stream", mode = Mode.WRITE)
    @Description("CALL algo.bfs.stream(label:String, relationshipType:String, startNodeId:long, direction:Direction, " +
            "{writeProperty:String, target:long, maxDepth:long, weightProperty:String, maxCost:double}) YIELD nodeId")
    public Stream<Path> bfs(
            @Name(value = "label") String label,
            @Name(value = "relationshipType") String relationship,
            @Name(value = "direction") Direction direction,
            @Name(value = "startNodeId") long startNode,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        final Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .withOptionalLabel(label)
                .withOptionalRelationshipType(relationship)
                .withoutNodeWeights()
                .withOptionalRelationshipWeightsFromProperty(configuration.getWeightProperty(), 1.)
                .asUndirected(true)
                .withLog(log)
                .load(configuration.getGraphImpl(HugeGraph.TYPE));
        final int source = graph.toMappedNodeId(startNode);
        final Traversal traversal = new Traversal(graph)
                .withProgressLogger(ProgressLogger.wrap(log, "BFS"))
                .withTerminationFlag(TerminationFlag.wrap(transaction));
        final long targetNode = configuration.getNumber("target", -1L).longValue();
        final long maxDepth = configuration.getNumber("maxDepth", -1L).longValue();
        final int mappedTargetNode = targetNode == -1L ? -1 : graph.toMappedNodeId(targetNode);
        final Traversal.Predicate exitFunction;
        final Traversal.Aggregator aggregatorFunction;

        // target node given; terminate if target is reached
        if (mappedTargetNode != -1) {
            exitFunction = (s, t, w) -> t == mappedTargetNode ? Traversal.Predicate.Result.BREAK : Traversal.Predicate.Result.FOLLOW;
            aggregatorFunction = (s, t, w) -> .0;

        // maxDepth given; continue to aggregate nodes with lower depth until no more nodes left
        } else if (maxDepth != -1) {
            exitFunction = (s, t, w) -> w >= maxDepth ? Traversal.Predicate.Result.CONTINUE : Traversal.Predicate.Result.FOLLOW;
            aggregatorFunction = (s, t, w) -> w + 1.;

        // maxCost & weightProperty given; aggregate nodes with lower cost then maxCost
        } else if (configuration.hasWeightProperty() && configuration.containsKeys("maxCost")) {
            final double maxCost = configuration.getNumber("maxCost", 1.).doubleValue();
            exitFunction = (s, t, w) -> w >= maxCost ? Traversal.Predicate.Result.CONTINUE : Traversal.Predicate.Result.FOLLOW;
            aggregatorFunction = (s, t, w) -> w + graph.weightOf(s, t);

        // do complete BFS until all nodes have been visited
        } else {
            exitFunction = (s, t, w) -> Traversal.Predicate.Result.FOLLOW;
            aggregatorFunction = (s, t, w) -> .0;
        }

        final long[] nodes = traversal.computeBfs(
                source,
                configuration.getDirection(Direction.OUTGOING),
                exitFunction,
                aggregatorFunction);

        return Stream.of(WalkPath.toPath(api, nodes));
    }

    @Procedure(value = "algo.dfs.stream", mode = Mode.WRITE)
    @Description("CALL algo.dfs.stream(label:String, relationshipType:String, startNodeId:long, direction:Direction, " +
            "{writeProperty:String, target:long, maxDepth:long, weightProperty:String, maxCost:double}) YIELD nodeId")
    public Stream<Path> dfs(
            @Name(value = "label") String label,
            @Name(value = "relationshipType") String relationship,
            @Name(value = "direction") Direction direction,
            @Name(value = "startNodeId") long startNode,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        final Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .withOptionalLabel(label)
                .withOptionalRelationshipType(relationship)
                .withoutNodeWeights()
                .withOptionalRelationshipWeightsFromProperty(configuration.getWeightProperty(), 1.)
                .asUndirected(true)
                .withLog(log)
                .load(configuration.getGraphImpl(HugeGraph.TYPE));
        final int source = graph.toMappedNodeId(startNode);
        final Traversal traversal = new Traversal(graph)
                .withProgressLogger(ProgressLogger.wrap(log, "BFS"))
                .withTerminationFlag(TerminationFlag.wrap(transaction));
        final long targetNode = configuration.getNumber("target", -1L).longValue();
        final long maxDepth = configuration.getNumber("maxDepth", -1L).longValue();
        final int mappedTargetNode = targetNode == -1L ? -1 : graph.toMappedNodeId(targetNode);
        final Traversal.Predicate exitFunction;
        final Traversal.Aggregator aggregatorFunction;

        // target node given; terminate if target is reached
        if (mappedTargetNode != -1) {
            exitFunction = (s, t, w) -> t == mappedTargetNode ? Traversal.Predicate.Result.BREAK : Traversal.Predicate.Result.FOLLOW;
            aggregatorFunction = (s, t, w) -> .0;

        // maxDepth given; continue to aggregate nodes with lower depth until no more nodes left
        } else if (maxDepth != -1) {
            exitFunction = (s, t, w) -> w >= maxDepth ? Traversal.Predicate.Result.CONTINUE : Traversal.Predicate.Result.FOLLOW;
            aggregatorFunction = (s, t, w) -> w + 1.;

        // maxCost & weightProperty given; aggregate nodes with lower cost then maxCost
        } else if (configuration.hasWeightProperty() && configuration.containsKeys("maxCost")) {
            final double maxCost = configuration.getNumber("maxCost", 1.).doubleValue();
            exitFunction = (s, t, w) -> w >= maxCost ? Traversal.Predicate.Result.CONTINUE : Traversal.Predicate.Result.FOLLOW;
            aggregatorFunction = (s, t, w) -> w + graph.weightOf(s, t);

        // do complete BFS until all nodes have been visited
        } else {
            exitFunction = (s, t, w) -> Traversal.Predicate.Result.FOLLOW;
            aggregatorFunction = (s, t, w) -> .0;
        }

        final long[] nodes = traversal.computeDfs(
                source,
                configuration.getDirection(Direction.OUTGOING),
                exitFunction,
                aggregatorFunction);

        return Stream.of(WalkPath.toPath(api, nodes));
    }

}
