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

public class PropertyMapping {

    // property name in the result map Graph.nodeProperties( <propertyName> )
    public final String propertyName;
    // property name in the graph (a:Node {<propertyKey>:xyz})
    public final String propertyKey;
    public final double defaultValue;

    public PropertyMapping(String propertyName, String propertyKeyInGraph, double defaultValue) {
        this.propertyName = propertyName;
        this.propertyKey = propertyKeyInGraph;
        this.defaultValue = defaultValue;
    }

    public static PropertyMapping of(String propertyName, String propertyKeyInGraph, double defaultValue) {
        return new PropertyMapping(propertyName, propertyKeyInGraph, defaultValue);
    }
}
