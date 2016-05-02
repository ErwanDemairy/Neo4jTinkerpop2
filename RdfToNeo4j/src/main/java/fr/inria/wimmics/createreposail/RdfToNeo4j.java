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
import java.util.Optional;
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
public class RdfToNeo4j {

	public static final String LITERAL = "literal";
	public static final String IRI = "IRI";
	public static final String BNODE = "bnode";
	public static final String CONTEXT = "context";
	public static final String KIND = "kind";
	public static final String LANG = "lang";
	public static final String TYPE = "type";
	public static final String VALUE = "value";

	private static Logger LOGGER = Logger.getLogger(RdfToNeo4j.class.getName());
	protected Model model;
	protected GraphDatabaseService graphDb;

	public static void main(String[] args) throws FileNotFoundException {
		if (args.length != 2) {
			System.err.println("Usage: rdfToNeo4j fileName.rdf neo4jdb_path");
			System.exit(1);
		}
		String rdfFileName = args[0];
		FileInputStream inputStream = new FileInputStream(new File(rdfFileName));
		String neo4jDbPath = args[1];

		RdfToNeo4j converter = new RdfToNeo4j();
		Optional<RDFFormat> format = Rio.getParserFormatForFileName(rdfFileName);
		if (format.isPresent()) {
			converter.convert(inputStream, format.get(), neo4jDbPath);
		} else {
			System.err.println("Format of the input file unkown.");
		}
	}

	/**
	 * Read a RDF stream and serialize it inside a Neo4j graph.
	 *
	 * @param rdfStream
	 * @param dbPath
	 */
	public void convert(InputStream rdfStream, RDFFormat format, String dbPath) {
		try {
			openNeo4jDb(dbPath);
			readFile(rdfStream, format);
			writeModelToNeo4j();
			closeDb();
		} catch (IOException ex) {
			Logger.getLogger(RdfToNeo4j.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	public void openNeo4jDb(String dbPath) {
		try {
			File dbDir = new File(dbPath);
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

	public void closeDb() {
		graphDb.shutdown();
	}

	/**
	 * Fill model with the content of an input stream.
	 *
	 * @param in Stream on an RDF file.
	 * @throws IOException
	 */
	public void readFile(InputStream in, RDFFormat format) throws IOException {
		RDFParser rdfParser = Rio.createParser(format);
		ParserConfig config = new ParserConfig();
		config.set(BasicParserSettings.PRESERVE_BNODE_IDS, true);
		rdfParser.setParserConfig(config);
		model = new org.openrdf.model.impl.LinkedHashModel();
		rdfParser.setRDFHandler(new StatementCollector(model));
		rdfParser.parse(in, "");
	}

	private boolean equalLiterals(Literal l, Node currentNode) {
		boolean result = true;
		result &= l.getLabel().equals(currentNode.getLabels().iterator().next().name());

		if (l.getLanguage().isPresent() && currentNode.hasProperty(LANG)) {
			String nodeLang = (String) currentNode.getProperty(LANG);
			result &= nodeLang.equals(l.getLanguage().get());
		} else {
			result &= !(l.getLanguage().isPresent() ^ currentNode.hasProperty(LANG));
		}
		result &= l.getDatatype().toString().equals(currentNode.getProperty(TYPE));
		return result;
	}

	protected boolean nodeEquals(Node endNode, Value object) {
		boolean result = true;
		result &= endNode.getProperty(KIND).equals(getKind(object));
		if (result) {
			switch (getKind(object)) {
				case BNODE:
				case IRI:
					result &= endNode.getProperty(VALUE).equals(object.stringValue());
					break;
				case LITERAL:
					Literal l = (Literal) object;
					result &= endNode.getProperty(VALUE).equals(l.getLabel());
					result &= endNode.getProperty(TYPE).equals(l.getDatatype().stringValue());
					if (l.getLanguage().isPresent()) {
						result &= endNode.hasProperty(LANG) && endNode.getProperty(LANG).equals(l.getLanguage().get());
					} else {
						result &= !endNode.hasProperty(LANG);
					}
			}
		}
		return result;
	}

	private static enum RelTypes implements RelationshipType {
		CONTEXT
	}

	public void writeModelToNeo4j() {
		Transaction tx = graphDb.beginTx();
		int triples = 0;
		for (Statement statement : model) {
			Resource context = statement.getContext();
			Resource source = statement.getSubject();
			IRI predicat = statement.getPredicate();
			Value object = statement.getObject();

			String contextString = (context == null) ? "" : context.stringValue();

			Node sourceNode = createNode(source);
			Node objectNode = createNode(object);

			RelationshipType propertyType = DynamicRelationshipType.withName(predicat.stringValue());
			Iterable<Relationship> relations = sourceNode.getRelationships(propertyType, Direction.OUTGOING);
			boolean relationFound = false;
			for (Relationship relation : relations) {
				if (relation.getProperty(CONTEXT).toString().equals(contextString) && nodeEquals(relation.getEndNode(), object)) {
					relationFound = true;
					break; // the relation already exists, so it has not to be added.
				}
			}
			if (!relationFound) {
				Relationship relation = sourceNode.createRelationshipTo(objectNode, DynamicRelationshipType.withName(predicat.stringValue()));
				relation.setProperty(CONTEXT, contextString);
			}
			triples++;
		}
		tx.success();
		tx.close();
		System.out.println(triples + " processed");
	}

	/**
	 * Returns a new node if v does not exist yet.
	 *
	 * @param v
	 * @param context
	 * @return
	 */
	private Node createNode(Value v) {
		Node result = null;
		Label label;
		Iterator<Node> nodes;
		switch (getKind(v)) {
			case IRI:
			case BNODE:
				label = DynamicLabel.label(v.stringValue());
				nodes = graphDb.findNodes(label);
				if (!nodes.hasNext()) {
					result = graphDb.createNode(label);
					result.setProperty(VALUE, v.stringValue());
					result.setProperty(KIND, getKind(v));
				} else {
					result = nodes.next();
				}
				break;
			case LITERAL:
				Literal l = (Literal) v;
				label = DynamicLabel.label(l.getLabel());
				nodes = graphDb.findNodes(label, VALUE, l.stringValue());
				boolean nodeFound = false;
				while (nodes.hasNext()) {
					Node currentNode = nodes.next();
					if (equalLiterals(l, currentNode)) {
						result = currentNode;
						nodeFound = true;
						break;
					}
				}
				if (!nodeFound) {
					result = graphDb.createNode(label);
					result.setProperty(VALUE, l.getLabel());
					result.setProperty(TYPE, l.getDatatype().toString());
					result.setProperty(KIND, getKind(v));
					if (l.getLanguage().isPresent()) {
						result.setProperty(LANG, l.getLanguage().get());
					}
				}
				break;
		}
		return result;
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

}
