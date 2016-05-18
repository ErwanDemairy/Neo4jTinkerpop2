/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fr.inria.wimmics.gremlin;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 *
 * @author edemairy
 */
public class GremlinHelloWorld {

	public static void main(String[] args) {
		Configuration config = new BaseConfiguration();
		config.setProperty(Neo4jGraph.CONFIG_DIRECTORY, "/Users/edemairy/Documents/Neo4j/default.graphdb");
		config.setProperty("gremlin.neo4j.conf.cache_type", "none");
		Graph graph = Neo4jGraph.open(config);
	}
}
