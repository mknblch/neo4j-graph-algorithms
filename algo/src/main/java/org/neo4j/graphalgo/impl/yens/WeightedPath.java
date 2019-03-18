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
package org.neo4j.graphalgo.impl.yens;

import org.apache.lucene.util.ArrayUtil;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipWeights;
import org.neo4j.graphalgo.core.utils.RawValues;

import java.util.Arrays;
import java.util.Comparator;
import java.util.function.IntConsumer;
import java.util.function.IntPredicate;

/**
 * array based path of nodes & a weight.
 *
 * hugification:
 *
 *  - long array / list : nodes
 *
 *
 * @author mknblch
 */
public class WeightedPath {

    public interface EdgeConsumer {
        void accept(int sourceNode, int targetNode);
    }

    private int[] nodes;
    private int offset = 0;
    private double weight = .0;

    public WeightedPath(int initialCapacity) {
        this(new int[initialCapacity], 0);
    }

    public WeightedPath(int[] data, int offset) {
        nodes = data;
        this.offset = offset;
    }


    /**
     * append a node to the path
     * @param nodeId
     */
    public void append(int nodeId) {
        nodes = ArrayUtil.grow(nodes, offset + 1);
        nodes[offset++] = nodeId;
    }

    /**
     * drop last element
     * @return this
     */
    public WeightedPath dropTail() {
        offset--;
        return this;
    }

    /**
     * return node id at index
     * @param index
     * @return
     */
    public int node(int index) {
        return nodes[index];
    }

    /**
     * number of nodes in this path
     * @return
     */
    public int size() {
        return offset;
    }

    /**
     * set accumulated weight for this path
     * @param weight
     * @return
     */
    public WeightedPath withWeight(double weight) {
        this.weight = weight;
        return this;
    }

    /**
     * check if this path contains a given node id
     * @param node
     * @return
     */
    public boolean containsNode(int node) {
        for (int i : nodes) {
            if (i == node) {
                return true;
            }
        }
        return false;
    }

    /**
     * consumer based iteration over nodes
     * @param consumer
     */
    public void forEach(IntPredicate consumer) {
        for (int i = 0; i < offset; i++) {
            if (!consumer.test(nodes[i])) {
                return;
            }
        }
    }

    /**
     * consumer based iteration over all edges of this path
     * @param consumer
     */
    public void forEachEdge(EdgeConsumer consumer) {
        for (int i = 0; i < offset - 1; i++) {
            consumer.accept(nodes[i], nodes[i + 1]);
        }
    }

    /**
     * consumer based iteration over nodes
     * @param consumer
     */
    public void forEachDo(IntConsumer consumer) {
        for (int i = 0; i < offset; i++) {
            consumer.accept(nodes[i]);
        }
    }

    /**
     * calculate weight of the path by summing all weights of all relations
     * @param weights
     * @return
     */
    public WeightedPath evaluateAndSetCost(RelationshipWeights weights) {
        this.weight = 0.;
        forEachEdge((a, b) -> {
            this.weight += weights.weightOf(a, b);
        });
        return this;
    }

    /**
     * get weight / costs for the path
     * @return
     */
    public double getCost() {
        return weight;
    }

    /**
     * create sub path from startNode to node at index
     * @param index
     * @return
     */
    public WeightedPath pathTo(int index) {
        if (index > size()) {
            throw new ArrayIndexOutOfBoundsException();
        }
        return new WeightedPath(Arrays.copyOf(nodes, index + 1), index + 1);
    }

    /**
     * retrieve the i'th edge of the path
     * @param i
     * @return
     */
    public long edge(int i) {
        return RawValues.combineIntInt(nodes[i], nodes[i + 1]);
    }

    /**
     * clear path
     */
    public void clear() {
        offset = 0;
    }

    /**
     * reverse the path
     */
    public WeightedPath reverse() {
        for (int i = 0; i < offset / 2; i++) {
            int temp = nodes[i];
            nodes[i] = nodes[offset - 1 - i];
            nodes[offset - 1 - i] = temp;
        }
        return this;
    }

    public String toString() {
        final StringBuilder buffer = new StringBuilder(size());
        forEachDo(n -> {
            if (buffer.length() != 0) {
                buffer.append(",");
            }
            buffer.append(n);
        });
        return buffer.toString();
    }

    public boolean elementWiseEquals(WeightedPath other, int length) {
        if (length == 0) {
            throw new IllegalArgumentException("length == 0");
        }
        if (size() < length || other.size() < length) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            if (nodes[i] != other.nodes[i]) {
                return false;
            }
        }
        return true;
    }

    public int[] toArray() {
        return Arrays.copyOf(nodes, offset);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WeightedPath that = (WeightedPath) o;

        if (offset != that.offset) return false;
        return Arrays.equals(nodes, that.nodes);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(nodes);
        result = 31 * result + offset;
        return result;
    }

    /**
     * concatenates another path to the and of this path
     * @return this
     */
    public WeightedPath concat(WeightedPath other) {
        nodes = ArrayUtil.grow(nodes, offset + other.size());
        System.arraycopy(other.nodes, 0, nodes, offset, other.size());
        offset += other.size();
        this.weight += other.weight;
        return this;
    }

    public static Comparator<WeightedPath> comparator() {
        return Comparator.comparingDouble(WeightedPath::getCost);
    }
}
