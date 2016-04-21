/*
 * Copyright Inria 2016. 
 */
package fr.inria.wimmics.createreposail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.openrdf.model.BNode;
import org.openrdf.model.IRI;
import org.openrdf.model.Literal;
import org.openrdf.model.Model;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.rio.ParserConfig;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.BasicParserSettings;
import org.openrdf.rio.helpers.StatementCollector;
import java.util.zip.GZIPInputStream;
import org.neo4j.graphdb.Direction;

/**
 * Tool that: (i) read a RDF file using sesame library; (ii) write the content
 * into a neo4j DB using the neo4j library.
 *
 * @author edemairy
 */
public class CreateRepo implements Runnable {

	public static final String LITERAL = "literal";
	public static final String IRI = "IRI";
	public static final String BNODE = "bnode";

	private static Logger LOGGER = Logger.getLogger(CreateRepo.class.getName());
	private Model model;
	private String DB_PATH = "/Users/edemairy/Documents/Neo4j/neo233.graphdb";
	private GraphDatabaseService graphDb;

	public static void main(String[] args) {
		CreateRepo object = new CreateRepo();
		object.run();
	}

	public void openNeo4jDb() {
		try {
			File dbDir = new File(DB_PATH);
			graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(dbDir);
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					graphDb.shutdown();
				}
			});
		} catch (Exception e) {
			LOGGER.severe(e.toString());
			e.printStackTrace();
		}

	}

	public void readFile() throws FileNotFoundException, IOException {
		File file = new File("/Users/edemairy/Documents/BTC/btc-2010-chunk-000");
//		InputStream in = new GZIPInputStream(new FileInputStream(file));
		InputStream in = new FileInputStream(file);
		RDFParser rdfParser = Rio.createParser(RDFFormat.NQUADS);
		ParserConfig config = new ParserConfig();
		config.set(BasicParserSettings.PRESERVE_BNODE_IDS, true);
		rdfParser.setParserConfig(config);
		model = new org.openrdf.model.impl.LinkedHashModel();
		rdfParser.setRDFHandler(new StatementCollector(model));
		rdfParser.parse(in, "");
	}

	private static enum RelTypes implements RelationshipType {
		CONTEXT
	}

	private static final String CONTEXT = "context";
	private static final String KIND = "kind";

	public void writeModelToNeo4j() {
		Transaction tx = graphDb.beginTx();
		for (Statement statement : model) {
			Resource context = statement.getContext();
			Resource source = statement.getSubject();
			IRI predicat = statement.getPredicate();
			Value object = statement.getObject();

			String contextString = context.stringValue();

			Node sourceNode = createNode(source, contextString);
			Node objectNode = createNode(object, contextString);

			RelationshipType propertyType = DynamicRelationshipType.withName(predicat.stringValue());
			Relationship relation = sourceNode.getSingleRelationship(propertyType, Direction.OUTGOING);
			if (relation == null) {
				relation = sourceNode.createRelationshipTo(objectNode, DynamicRelationshipType.withName(predicat.stringValue()));
			}
			relation.setProperty(CONTEXT, contextString);
		}
		tx.success();
		tx.close();
	}

	/**
	 * Returns a new node if v does not exist yet.
	 *
	 * @param v
	 * @param context
	 * @return
	 */
	private Node createNode(Value v, String context) {
		Label label = DynamicLabel.label(v.stringValue());
		Iterator<Node> nodes = graphDb.findNodes(label);
		Node node = (nodes.hasNext()) ? nodes.next() : graphDb.createNode(label);
		node.setProperty(CONTEXT, context);
		node.setProperty(KIND, getKind(v));
		return node;
	}

	private static String getKind(Value resource) {
		if (isLiteral(resource)) {
			return LITERAL;
		} else if (isIRI(resource)) {
			return IRI;
		} else if (isBNode(resource)) {
			return BNODE;
		}
		throw new IllegalArgumentException("Impossible to find the type of:" + resource.stringValue());
	}

	private static boolean isType(Class c, Object o) {
		try {
			c.cast(o);
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	private static boolean isLiteral(Value resource) {
		return isType(Literal.class, resource);
	}

	private static boolean isIRI(Value resource) {
		return isType(IRI.class, resource);
	}

	private static boolean isBNode(Value resource) {
		return isType(BNode.class, resource);
	}

	@Override
	public void run() {
		try {
			readFile();
			openNeo4jDb();
			writeModelToNeo4j();
		} catch (IOException ex) {
			LOGGER.log(Level.SEVERE, null, ex);
		}

	}
}
