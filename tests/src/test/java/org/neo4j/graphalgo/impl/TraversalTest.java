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
package org.neo4j.graphalgo.impl;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.impl.walking.WalkResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 *
 * Graph:
 *
 *     (b)   (e)
 *    /  \  /  \
 * >(a)  (d)   (g)
 *    \  /  \  /
 *    (c)   (f)
 *
 * @author mknblch
 */
public class TraversalTest {

    @ClassRule
    public static ImpermanentDatabaseRule db = new ImpermanentDatabaseRule();

    private static Graph graph;

    @BeforeClass
    public static void setupGraph() throws KernelException {

        final String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                        "CREATE (b:Node {name:'b'})\n" +
                        "CREATE (c:Node {name:'c'})\n" +
                        "CREATE (d:Node {name:'d'})\n" +
                        "CREATE (e:Node {name:'e'})\n" +
                        "CREATE (f:Node {name:'f'})\n" +
                        "CREATE (g:Node {name:'g'})\n" +
                        "CREATE" +
                        " (a)-[:TYPE]->(b),\n" +
                        " (a)-[:TYPE]->(c),\n" +
                        " (b)-[:TYPE]->(d),\n" +
                        " (c)-[:TYPE]->(d),\n" +
                        " (d)-[:TYPE]->(e),\n" +
                        " (d)-[:TYPE]->(f),\n" +
                        " (e)-[:TYPE]->(g),\n" +
                        " (f)-[:TYPE]->(g)";

        db.execute(cypher);

        graph = new GraphLoader(db)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .withRelationshipWeightsFromProperty("cost", Double.MAX_VALUE)
                .withDirection(Direction.BOTH)
                .load(HeavyGraphFactory.class);

    }

    private static int id(String name) {
        final Node[] node = new Node[1];
        db.execute("MATCH (n:Node) WHERE n.name = '" + name + "' RETURN n").accept(row -> {
            node[0] = row.getNode("n");
            return false;
        });
        return graph.toMappedNodeId(node[0].getId());
    }

    @Test
    public void testBfsOutgoing() throws Exception {
        final Traversal algo = new Traversal(graph);
        final long[] result = algo.computeBfs(id("a"), Direction.OUTGOING, id("g"), Integer.MAX_VALUE);
        System.out.println("result.nodeIds = " + Arrays.toString(result));
        assertEquals(7, result.length);
    }

    @Test
    public void testDfsOutgoing() throws Exception {
        final Traversal algo = new Traversal(graph);
        final long[] result = algo.computeDfs(id("a"), Direction.OUTGOING, id("g"), Integer.MAX_VALUE);
        System.out.println("result.nodeIds = " + Arrays.toString(result));
        assertEquals(5, result.length);
    }


    @Test
    public void testBfsBoth() throws Exception {
        final Traversal algo = new Traversal(graph);
        final long[] result = algo.computeBfs(id("a"), Direction.BOTH, id("g"), Integer.MAX_VALUE);
        System.out.println("result.nodeIds = " + Arrays.toString(result));
        assertEquals(7, result.length);
    }

    @Test
    public void testDfsBoth() throws Exception {
        final Traversal algo = new Traversal(graph);
        final long[] result = algo.computeDfs(id("a"), Direction.BOTH, id("g"), Integer.MAX_VALUE);
        System.out.println("result.nodeIds = " + Arrays.toString(result));
        assertEquals(5, result.length);
    }

    @Test
    public void testBfsIncoming() throws Exception {
        final Traversal algo = new Traversal(graph);
        final long[] result = algo.computeBfs(id("g"), Direction.INCOMING, id("a"), Integer.MAX_VALUE);
        System.out.println("result.nodeIds = " + Arrays.toString(result));
        assertEquals(7, result.length);
    }

    @Test
    public void testDfsIncoming() throws Exception {
        final Traversal algo = new Traversal(graph);
        final long[] result = algo.computeDfs(id("g"), Direction.INCOMING, id("a"), Integer.MAX_VALUE);
        System.out.println("result.nodeIds = " + Arrays.toString(result));
        assertEquals(5, result.length);
    }

}
