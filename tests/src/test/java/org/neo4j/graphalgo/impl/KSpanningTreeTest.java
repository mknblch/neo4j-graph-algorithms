package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.procedures.IntProcedure;
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
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *          1
 *      (a)---(d)       (a)   (d)
 *      /3 \2 /3   =>   /     /
 *    (b)---(c)       (b)   (c)
 *        1
 *
 * @author mknblch
 */
@RunWith(Parameterized.class)
public class KSpanningTreeTest {

    private static final String cypher =
            "CREATE (a:Node {name:'a'})\n" +
                    "CREATE (b:Node {name:'b'})\n" +
                    "CREATE (c:Node {name:'c'})\n" +
                    "CREATE (d:Node {name:'d'})\n" +

                    "CREATE" +
                    " (a)-[:TYPE {w:3.0}]->(b),\n" +
                    " (a)-[:TYPE {w:2.0}]->(c),\n" +
                    " (a)-[:TYPE {w:1.0}]->(d),\n" +
                    " (b)-[:TYPE {w:1.0}]->(c),\n" +
                    " (d)-[:TYPE {w:3.0}]->(c)";

    @ClassRule
    public static final ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return Collections.singleton(new Object[]{HugeGraphFactory.class, "Huge"});
//        return Arrays.asList(
//                new Object[]{HeavyGraphFactory.class, "Heavy"},
//                new Object[]{LightGraphFactory.class, "Light"},
//                new Object[]{HugeGraphFactory.class, "Huge"},
//                new Object[]{GraphViewFactory.class, "View"}
//        );
    }

    private int a, b, c, d;

    @BeforeClass
    public static void setupGraph() throws KernelException {
        DB.execute(cypher);
    }

    private Graph graph;

    public KSpanningTreeTest(
            Class<? extends GraphFactory> graphImpl,
            String nameIgnoredOnlyForTestName) {
        graph = new GraphLoader(DB)
                .withRelationshipWeightsFromProperty("w", 1.0)
                .withAnyRelationshipType()
                .withAnyLabel()
                .asUndirected(true)
                .load(graphImpl);

        try (Transaction tx = DB.beginTx()) {
            a = graph.toMappedNodeId(DB.findNode(Label.label("Node"), "name", "a").getId());
            b = graph.toMappedNodeId(DB.findNode(Label.label("Node"), "name", "b").getId());
            c = graph.toMappedNodeId(DB.findNode(Label.label("Node"), "name", "c").getId());
            d = graph.toMappedNodeId(DB.findNode(Label.label("Node"), "name", "d").getId());
            tx.success();
        };
    }

    @Test
    public void testMaximumKSpanningTree() throws Exception {
        final DisjointSetStruct struct = new KSpanningTree(graph, graph, graph)
                .compute(a, 2, true)
                .getSetStruct();
        assertTrue(struct.connected(a, b));
        assertTrue(struct.connected(c, d));
        assertFalse(struct.connected(a, c));
    }

    @Test
    public void testMinimumKSpanningTree() throws Exception {
        final DisjointSetStruct struct = new KSpanningTree(graph, graph, graph)
                .compute(a, 2, false)
                .getSetStruct();
        assertTrue(struct.connected(a, d));
        assertTrue(struct.connected(b, c));
        assertFalse(struct.connected(a, b));
    }
}
