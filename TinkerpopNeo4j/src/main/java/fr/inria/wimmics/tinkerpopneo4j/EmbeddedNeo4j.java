/*
 * use directly the neo4j api. 
 */
package fr.inria.wimmics.tinkerpopneo4j;

import java.io.IOException;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

public class EmbeddedNeo4j {

	private static final String DB_PATH = "/Users/edemairy/Documents/Neo4j/default.graphdb";

	public String greeting;

	// START SNIPPET: vars
	GraphDatabaseService graphDb;
	Node firstNode;
	Node secondNode;
	Relationship relationship;
	// END SNIPPET: vars

	private void dumpDb() {
		Iterable<Node> nodes = GlobalGraphOperations.at(graphDb).getAllNodes();
		Iterable<Relationship> edges = GlobalGraphOperations.at(graphDb).getAllRelationships();
		for (Relationship e: edges) {
			Node first = e.getStartNode();
			Node second = e.getEndNode();
			System.out.println( "(" + first.getProperty("value") + ") - [" + e.toString() + "] - (" + second.getProperty("value") + ")");
		}
	}

	// START SNIPPET: createReltype
	private static enum RelTypes implements RelationshipType {
		KNOWS
	}
	// END SNIPPET: createReltype

	public static void main(final String[] args) throws IOException {
		EmbeddedNeo4j hello = new EmbeddedNeo4j();
		hello.openDb();
		hello.createDb();
//		hello.removeData();
		hello.dumpDb();
		hello.shutDown();
	}

	void openDb() throws IOException {
//		FileUtils.deleteRecursively(new File(DB_PATH));
		// START SNIPPET: startDb
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
		registerShutdownHook(graphDb);
		// END SNIPPET: startDb
	}
	void createDb() throws IOException {


		// START SNIPPET: transaction
		Transaction tx = graphDb.beginTx();
		// Database operations go here
		// END SNIPPET: transaction
		// START SNIPPET: addData
		firstNode = graphDb.createNode();
		firstNode.setProperty("message", "Hello, ");
		secondNode = graphDb.createNode();
		secondNode.setProperty("message", "World!");

		relationship = firstNode.createRelationshipTo(secondNode, RelTypes.KNOWS);
		relationship.setProperty("message", "brave Neo4j ");
		// END SNIPPET: addData

		// START SNIPPET: readData
		System.out.print(firstNode.getProperty("message"));
		System.out.print(relationship.getProperty("message"));
		System.out.print(secondNode.getProperty("message"));
		// END SNIPPET: readData

		greeting = ((String) firstNode.getProperty("message"))
			+ ((String) relationship.getProperty("message"))
			+ ((String) secondNode.getProperty("message"));

		// START SNIPPET: transaction
		tx.success();

		// END SNIPPET: transaction
	}

	void removeData() {
		Transaction tx = graphDb.beginTx();
		// START SNIPPET: removingData
		// let's remove the data
		firstNode.getSingleRelationship(RelTypes.KNOWS, Direction.OUTGOING).delete();
		firstNode.delete();
		secondNode.delete();
		// END SNIPPET: removingData

		tx.success();
	}

	void shutDown() {
		System.out.println();
		System.out.println("Shutting down database ...");
		// START SNIPPET: shutdownServer
		graphDb.shutdown();
		// END SNIPPET: shutdownServer
	}

	// START SNIPPET: shutdownHook
	private static void registerShutdownHook(final GraphDatabaseService graphDb) {
		// Registers a shutdown hook for the Neo4j instance so that it
		// shuts down nicely when the VM exits (even if you "Ctrl-C" the
		// running application).
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.shutdown();
			}
		});
	}
	// END SNIPPET: shutdownHook
}
