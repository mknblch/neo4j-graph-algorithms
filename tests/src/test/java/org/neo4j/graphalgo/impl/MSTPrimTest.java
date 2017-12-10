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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.huge.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.container.UndirectedTree;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests if MSTPrim returns a valid MST for each node
 *
 *         a                a
 *     1 /   \ 2          /  \
 *      /     \          /    \
 *     b --3-- c        b      c
 *     |       |   =>   |      |
 *     4       5        |      |
 *     |       |        |      |
 *     d --6-- e        d      e
 *
 *
 * @author mknobloch
 */
@RunWith(Parameterized.class)
public class MSTPrimTest {

    private final Label label;
    private int a, b, c, d, e;

    private static final String cypher =
            "CREATE (a:Node {name:'a'})\n" +
            "CREATE (b:Node {name:'b'})\n" +
            "CREATE (c:Node {name:'c'})\n" +
            "CREATE (d:Node {name:'d'})\n" +
            "CREATE (e:Node {name:'e'})\n" +
            "CREATE" +
            " (a)-[:TYPE {cost:1.0}]->(b),\n" +
            " (a)-[:TYPE {cost:2.0}]->(c),\n" +
            " (b)-[:TYPE {cost:3.0}]->(c),\n" +
            " (b)-[:TYPE {cost:4.0}]->(d),\n" +
            " (c)-[:TYPE {cost:5.0}]->(e),\n" +
            " (d)-[:TYPE {cost:6.0}]->(e)";

    @ClassRule
    public static final ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    private final Graph graph;

    @BeforeClass
    public static void setupGraph() {
        try (Transaction tx = DB.beginTx()) {
            DB.execute(cypher);
            tx.success();
        }
    }


    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return Collections.singleton(new Object[]{HugeGraphFactory.class, "Huge"});
    }


    public MSTPrimTest(
            Class<? extends GraphFactory> graphImpl,
            String nameIgnoredOnlyForTestName) {

        label = Label.label("Node");

        graph = new GraphLoader(DB)
                .withLabel(label)
                .withRelationshipType("TYPE")
                .withRelationshipWeightsFromProperty("cost", Double.MAX_VALUE)
                .withoutNodeWeights()
                .withDirection(Direction.BOTH)
                .asUndirected(true)
                .load(graphImpl);

        try (Transaction transaction = DB.beginTx()) {
            a = graph.toMappedNodeId(DB.findNode(label, "name", "a").getId());
            b = graph.toMappedNodeId(DB.findNode(label, "name", "b").getId());
            c = graph.toMappedNodeId(DB.findNode(label, "name", "c").getId());
            d = graph.toMappedNodeId(DB.findNode(label, "name", "d").getId());
            e = graph.toMappedNodeId(DB.findNode(label, "name", "e").getId());
            transaction.success();
        }
    }

    @Test
    public void testMstFromA() throws Exception {
        verifyMst(new MSTPrim(graph).compute(a).getMinimumSpanningTree());
    }

    @Test
    public void testMstFromB() throws Exception {
        verifyMst(new MSTPrim(graph).compute(b).getMinimumSpanningTree());
    }

    @Test
    public void testMstFromC() throws Exception {
        verifyMst(new MSTPrim(graph).compute(c).getMinimumSpanningTree());
    }

    @Test
    public void testMstFromD() throws Exception {
        verifyMst(new MSTPrim(graph).compute(d).getMinimumSpanningTree());
    }

    @Test
    public void testMstFromE() throws Exception {
        verifyMst(new MSTPrim(graph).compute(d).getMinimumSpanningTree());
    }

    private void verifyMst(UndirectedTree mst) {
        final AssertingConsumer consumer = new AssertingConsumer();
        mst.forEachDFS(a, consumer);
        consumer.assertContains(a, b);
        consumer.assertContains(a, c);
        consumer.assertContains(b, d);
        consumer.assertContains(c, e);
    }

    private static class AssertingConsumer implements RelationshipConsumer {

        private static class Pair {

            final int a;
            final int b;

            private Pair(int a, int b) {
                this.a = a;
                this.b = b;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                Pair pair = (Pair) o;

                if (a != pair.a) return false;
                return b == pair.b;

            }

            @Override
            public int hashCode() {
                int result = a;
                result = 31 * result + b;
                return result;
            }
        }

        private ArrayList<Pair> pairs = new ArrayList<>();

        @Override
        public boolean accept(int sourceNodeId, int targetNodeId, @Deprecated long relationId) {
            pairs.add(new Pair(
                    Math.min(sourceNodeId, targetNodeId),
                    Math.max(sourceNodeId, targetNodeId)));
            return true;
        }

        public void assertSize(int expected) {
            assertEquals("size does not match", expected, pairs.size());
        }

        public void assertContains(int a, int b) {
            assertTrue("{" + a + "," + b + "} not found",
                    pairs.contains(new Pair(Math.min(a, b), Math.max(a, b))));
        }
    }
}
