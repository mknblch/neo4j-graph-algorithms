package org.neo4j.graphalgo.impl;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.HugeGraphFactory;
import org.neo4j.graphalgo.core.lightweight.LightGraphFactory;
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;
import org.neo4j.graphalgo.core.utils.container.UndirectedTree;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.Collection;

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
public class KMeansTest {

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
        return Arrays.asList(
                new Object[]{HeavyGraphFactory.class, "Heavy"},
                new Object[]{LightGraphFactory.class, "Light"},
                new Object[]{HugeGraphFactory.class, "Huge"},
                new Object[]{GraphViewFactory.class, "View"}
        );
    }

    private int a, b, c, d;

    @BeforeClass
    public static void setupGraph() throws KernelException {
        DB.execute(cypher);
    }

    private Graph graph;

    public KMeansTest(
            Class<? extends GraphFactory> graphImpl,
            String nameIgnoredOnlyForTestName) {
        graph = new GraphLoader(DB)
                .withRelationshipWeightsFromProperty("w", 1.0)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withDirection(Direction.BOTH)
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
    public void test() throws Exception {
        final DisjointSetStruct setStruct = new KMeans(graph, 2)
                .compute(a)
                .getSetStruct();

        System.out.println(setStruct);
    }

    @Test
    public void testMST() throws Exception {

        final UndirectedTree minimumSpanningTree = new MSTPrim(graph).compute(a)
                .getMinimumSpanningTree();

        minimumSpanningTree.forEachBFS(a, (s, t, r) -> {
            System.out.println(s + " -> " + t);
            return true;
        });

    }
}
