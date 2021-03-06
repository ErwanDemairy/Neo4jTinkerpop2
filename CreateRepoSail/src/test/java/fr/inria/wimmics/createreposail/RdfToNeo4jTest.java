/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fr.inria.wimmics.createreposail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import org.neo4j.tooling.GlobalGraphOperations;

/**
 *
 * @author edemairy
 */
public class RdfToNeo4jTest {

	private static final String ROOT_RESOURCES = "src/test/resources/";

	/**
	 * Test of convert method, of class RdfToNeo4j.
	 */
	@Test
	public void testConvert() throws FileNotFoundException {
		String inputFile = ROOT_RESOURCES + "testConvert/input1.rdf";
		String outputDb = ROOT_RESOURCES + "testConvertResult.neo4jdb";
		String expectedDb = ROOT_RESOURCES + "testConvertExpected.neo4jdb";

		RdfToNeo4j converter = new RdfToNeo4j();
		FileInputStream inputStream = new FileInputStream(new File(inputFile));
		converter.convert(inputStream, outputDb);
		GraphDatabaseService result = new GraphDatabaseFactory().newEmbeddedDatabase(new File(outputDb));
		GraphDatabaseService expected = new GraphDatabaseFactory().newEmbeddedDatabase(new File(expectedDb));
		assert (areEquals(result, expected));
		result.shutdown();
		expected.shutdown();
	}

	private boolean areEquals(GraphDatabaseService result, GraphDatabaseService expected) {
		result.beginTx();
		expected.beginTx();
		int nbResultNodes = count(GlobalGraphOperations.at(result).getAllNodes());
		int nbExpectedNodes = count(GlobalGraphOperations.at(expected).getAllNodes());
		assert (nbResultNodes == nbExpectedNodes);
		assert (count(GlobalGraphOperations.at(result).getAllRelationships()) == count(GlobalGraphOperations.at(expected).getAllRelationships()));
		for (Node n : GlobalGraphOperations.at(expected).getAllNodes()) {
			Iterable<Label> labels = n.getLabels();
			for (Label l : labels) {
				assert (result.findNodes(l) != null);
			}
		}
		return true;
	}

}
