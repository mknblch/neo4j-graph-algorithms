package org.neo4j.graphalgo.impl.infomap;

import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntObjectScatterMap;
import com.carrotsearch.hppc.IntScatterSet;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.carrotsearch.hppc.procedures.IntProcedure;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeWeights;
import org.neo4j.graphalgo.api.RelationshipWeights;
import org.neo4j.graphalgo.core.utils.DegreeNormalizedRelationshipWeights;
import org.neo4j.graphalgo.core.utils.NormalizedRelationshipWeights;
import org.neo4j.graphalgo.core.utils.Pointer;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphalgo.impl.pagerank.PageRankAlgorithm;
import org.neo4j.graphalgo.impl.pagerank.PageRankResult;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.BitSet;
import java.util.function.Consumer;
import java.util.stream.LongStream;

/**
 * Java adaption of InfoMap from https://github.com/felixfung/InfoFlow
 *
 * @author mknblch, Mark Schaarschmidt
 */
public class InfoMap extends Algorithm<InfoMap> {

    private static final double LOG2 = Math.log(2.);

    // default TAU
    public static final double TAU = 0.15;
    // default threshold
    public static final double THRESHOLD = .005;

    // iteration direction is constant since the graph should be undirected
    private static final Direction D = Direction.OUTGOING;
    // number of nodes
    private final int nodeCount;
    // constant TAU (0.15 was given in the original MapEq. paper https://arxiv.org/abs/0906.1405 )
    private final double tau;
    // absolute minimum difference in deltaL for joining  2 modules together
    private final double threshold;
    // the graph
    private Graph graph;
    // page rank weights
    private NodeWeights pageRanks;
    // degree normalized relationship weights (weights of one node must add up to 1.0)
    private RelationshipWeights weights;

    // following values are updated during a merge
    // number of iterations the last computation took
    private int iterations = 0;
    // module map
    private IntObjectMap<Module> modules;
    // community assignment helper array
    private int[] communities;
    // sum of exit probs.
    private double sQi = 0.;


    /**
     * create a weighted InfoMap algo instance
     */
    public static InfoMap weighted(Graph graph, int prIterations, RelationshipWeights weights) {
        final PageRankResult pageRankResult = PageRankAlgorithm.of(graph, 1. - TAU, LongStream.empty())
                .compute(prIterations)
                .result();
        return weighted(graph, pageRankResult::score, weights, THRESHOLD, TAU);
    }

    /**
     * create a weighted InfoMap algo instance with precalculated pageRanks
     */
    public static InfoMap weighted(Graph graph, NodeWeights pageRanks, RelationshipWeights weights) {
        return weighted(graph, pageRanks, weights, THRESHOLD, TAU);
    }

    /**
     * create a weighted InfoMap algo instance with precalculated pageRanks
     */
    public static InfoMap weighted(Graph graph, NodeWeights pageRanks, RelationshipWeights weights, double threshold, double tau) {
        return new InfoMap(
                graph,
                pageRanks,
                new NormalizedRelationshipWeights(Math.toIntExact(graph.nodeCount()), graph, weights),
                threshold,
                tau);
    }

    /**
     * create an unweighted InfoMap algo instance
     */
    public static InfoMap unweighted(Graph graph, int prIterations) {
        final PageRankResult pageRankResult = PageRankAlgorithm.of(graph, 1. - TAU, LongStream.empty())
                .compute(prIterations)
                .result();
        return unweighted(graph, pageRankResult::score, THRESHOLD, TAU);
    }

    /**
     * create an unweighted InfoMap algo instance with precalcualted pageRanks
     */
    public static InfoMap unweighted(Graph graph, NodeWeights pageRanks) {
        return unweighted(graph, pageRanks, THRESHOLD, TAU);
    }

    /**
     * create an unweighted InfoMap algo instance
     */
    public static InfoMap unweighted(Graph graph, NodeWeights pageRanks, double threshold, double tau) {
        return new InfoMap(
                graph,
                pageRanks,
                new DegreeNormalizedRelationshipWeights(graph),
                threshold,
                tau);
    }

    /**
     * @param graph graph
     * @param pageRanks precalculated page ranks
     * @param normalizedWeights normalized weights (weights of a node must sum up to 1.0)
     * @param threshold minimum delta L for optimization
     * @param tau constant tau (usually 0.15)
     */
    private InfoMap(Graph graph, NodeWeights pageRanks, RelationshipWeights normalizedWeights, double threshold, double tau) {
        this.graph = graph;
        this.pageRanks = pageRanks;
        this.weights = normalizedWeights;
        this.nodeCount = Math.toIntExact(graph.nodeCount());
        this.tau = tau;
        this.threshold = threshold;
        this.modules = new IntObjectScatterMap<>(nodeCount);
        this.communities = new int[nodeCount];
        Arrays.setAll(communities, i -> i);
        final double[] d = {0.};
        graph.forEachNode(node -> {
            final Module module = new Module(node);
            modules.put(node, module);
            d[0] += module.q;
            return true;
        });
        this.sQi = d[0];
    }

    /**
     * minimize description length
     */
    public InfoMap compute() {
        this.iterations = 0;
        while (optimize()) {
            this.iterations++;
        }
        return this;
    }

    @Override
    public InfoMap me() {
        return this;
    }

    /**
     * release data
     *
     * @return
     */
    @Override
    public InfoMap release() {
        graph = null;
        pageRanks = null;
        weights = null;
        modules = null;
        communities = null;
        return this;
    }

    /**
     * return community array
     *
     * @return
     */
    public int[] getCommunities() {
        return communities;
    }

    /**
     * number of distinct modules left
     *
     * @return number of modules
     */
    public int getCommunityCount() {
        return modules.size();
    }

    /**
     * return number of iterations
     *
     * @return number of iterations
     */
    public int getIterations() {
        return iterations;
    }

    /**
     * find a pair of modules that lead to the
     * best reduction in L and merge them as
     * long as their absolute difference is
     * higher then {@link InfoMap#threshold}
     *
     * @return true if a merge occurred, false otherwise
     */
    private boolean optimize() {
        final Pointer.DoublePointer m = Pointer.wrap(-threshold);
        final int[] best = {-1, -1};
        for (IntObjectCursor<Module> cursor : modules) {
            final Module module = cursor.value;
            forEachNeighboringModule(module, other -> {
                final double v = delta(module, other);
                if (v < m.v) {
                    m.v = v;
                    best[0] = module.index;
                    best[1] = other.index;
                }
            });
        }
        // merge module with higher i into mod with lower i
        if (best[0] == -1 || best[0] == best[1]) {
            return false;
        }
        if (best[0] < best[1]) {
            modules.get(best[0]).merge(modules.remove(best[1]));
        } else {
            modules.get(best[1]).merge(modules.remove(best[0]));
        }
        return true;
    }

    /**
     * call consumer for each connected community once
     *
     * @param m        the module
     * @param consumer module consumer
     */
    private void forEachNeighboringModule(Module m, Consumer<Module> consumer) {

        final BitSet bitSet = new BitSet(nodeCount);

        m.nodes.forEach((IntProcedure) node -> {
            final int cs = communities[node];
            // for each connected node t to this module
            graph.forEachOutgoing(node, (s, t, r) -> {
                final int community = communities[t];
                // same community
                if (cs == community) {
                    return true;
                }
                // already visited
                if (bitSet.get(community)) {
                    return true;
                }
                // do visit
                bitSet.set(community);
                consumer.accept(modules.get(community));
                return true;
            });
        });

    }

    /**
     * calculate deltaL, the change in L if module a and b are joined together
     *
     * @param a module a
     * @param b module b
     * @return delta L
     */
    private double delta(Module a, Module b) {
        double ni = a.nodes.size() + b.nodes.size();
        double pi = a.p + b.p;
        double wi = a.w + b.w - interModW(a, b);
        double qi = tau * ((double) nodeCount - ni) / (nodeCount - 1.) * pi + (1. - tau) * wi;
        return plogp(qi - a.q - b.q + sQi)
                - plogp(sQi)
                - 2 * plogp(qi)
                + 2 * plogp(a.q)
                + 2 * plogp(b.q)
                + plogp(pi + qi)
                - plogp(a.p + a.q)
                - plogp(b.p + b.q);
    }

    /**
     * sum of weights between 2 modules multiplied
     * by the pageRanks of their source nodes
     *
     * @param j module j
     * @param k module k
     * @return sum of shared weights from both ends
     */
    private double interModW(Module j, Module k) {
        final Pointer.DoublePointer w = Pointer.wrap(.0);
        j.nodes.forEach((IntProcedure) c -> {
            graph.forEachOutgoing(c, (s, t, r) -> {
                if (k.nodes.contains(t)) {
                    w.v += (pageRanks.weightOf(s) * weights.weightOf(s, t)) // =>
                            + (pageRanks.weightOf(t) * weights.weightOf(t, s)); // <=

                }
                return true;
            });
        });
        return w.v;
    }

    /**
     * a module represents a community
     */
    private class Module {
        // module id
        final int index;
        // nodes
        IntSet nodes;
        // ergodic frequency
        double p;
        // exit probability without teleport
        double w;
        // exit probability with teleport
        double q;

        public Module(int startNode) {
            this.index = startNode;
            nodes = new IntScatterSet();
            nodes.add(startNode);
            final Pointer.DoublePointer sumW = Pointer.wrap(.0);
            // sum of all weights
            graph.forEachRelationship(startNode, D, (s, t, r) -> {
                if (s != t) {
                    sumW.v += weights.weightOf(s, t);
                }
                return true;
            });
            p = pageRanks.weightOf(startNode);
            w = p * sumW.v;
            q = tau * p + (1. - tau) * w;
        }

        public void merge(Module other) {
            final int ni = nodes.size() + other.nodes.size();
            p += other.p;
            w += other.w - interModW(this, other);
            sQi -= q + other.q;
            q = tau * (((double) nodeCount - ni) / (nodeCount - 1.)) * p + (1. - tau) * w;
            sQi += q;
            other.nodes.forEach((IntProcedure) node -> {
                this.nodes.add(node);
                communities[node] = index;
            });
        }
    }

    private static double plogp(double v) {
        return v > .0 ? v * log2(v) : 0.;
    }

    private static double log2(double v) {
        return Math.log(v) / LOG2;
    }
}