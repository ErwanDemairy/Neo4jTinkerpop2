/*
 * Copyright Inria 2016. 
 */
package fr.inria.wimmics.createreposail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
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
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.openrdf.rio.helpers.NTriplesParserSettings;

/**
 * Tool that: (i) read a RDF file using sesame library; (ii) write the content
 * into a neo4j DB using the neo4j library.
 *
 * @author edemairy
 */
public class RdfToNeo4jBatch {

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
	protected BatchInserter graphDb;

	public static void main(String[] args) throws FileNotFoundException {
		if (args.length != 2) {
			System.err.println("Usage: rdfToNeo4j fileName.rdf neo4jdb_path");
			System.exit(1);
		}
		String rdfFileName = args[0];
		FileInputStream inputStream = new FileInputStream(new File(rdfFileName));
		String neo4jDbPath = args[1];

		RdfToNeo4jBatch converter = new RdfToNeo4jBatch();
		Optional<RDFFormat> format = Rio.getParserFormatForFileName(rdfFileName);
		if (format.isPresent()) {
			converter.convert(inputStream, format.get(), neo4jDbPath);
		} else {
			System.err.println("Format of the input file unkown.");
			converter.convert(inputStream, RDFFormat.NQUADS, neo4jDbPath);
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
			LOGGER.info("** begin of convert **");
			LOGGER.info("opening the db at "+dbPath);
			openNeo4jDb(dbPath);
			LOGGER.info("Loading file");
			readFile(rdfStream, format);
			LOGGER.info("Writing graph in db");
			writeModelToNeo4j();
			LOGGER.info("closing DB");
			closeDb();
			LOGGER.info("** end of convert **");
		} catch (IOException ex) {
			Logger.getLogger(RdfToNeo4j.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	public void openNeo4jDb(String dbPath) {
		try {
			File dbDir = new File(dbPath);
			graphDb = BatchInserters.inserter(dbDir);
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
		config.addNonFatalError(NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES);
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

	final static private int THRESHOLD = 9414890; //Integer.MAX_VALUE;
	public void writeModelToNeo4j() {
		int triples = 0;
		for (Statement statement : model) {
			Resource context = statement.getContext();
			Resource source = statement.getSubject();
			IRI predicat = statement.getPredicate();
			Value object = statement.getObject();

			String contextString = (context == null) ? "" : context.stringValue();

			long sourceNode = createNode(source);
			long objectNode = createNode(object);

			Map<String, Object> properties = new HashMap();
			properties.put(CONTEXT, contextString);
			long relation = graphDb.createRelationship(sourceNode, objectNode, DynamicRelationshipType.withName(predicat.stringValue()), properties);
			triples++;
			if (triples > THRESHOLD) {
				break;
			}
		}
		System.out.println(triples + " processed");
	}
	Map<String, Long> alreadySeen = new HashMap<>();

	/**
	 * Returns a new node if v does not exist yet.
	 *
	 * @param v
	 * @param context
	 * @return
	 */
	private long createNode(Value v) {
		long result = -1;
		if (alreadySeen.containsKey(v.stringValue())) {
			return alreadySeen.get(v.stringValue());
		}
		Label label;
		Map<String, Object> properties = new HashMap();
		switch (getKind(v)) {
			case IRI:
			case BNODE:
				label = DynamicLabel.label(v.stringValue());
				properties.put(VALUE, v.stringValue());
				properties.put(KIND, getKind(v));
				result = graphDb.createNode(properties, label);
				break;
			case LITERAL:
				Literal l = (Literal) v;
				label = DynamicLabel.label(l.getLabel());
				properties.put(VALUE, l.getLabel());
				properties.put(TYPE, l.getDatatype().toString());
				properties.put(KIND, getKind(v));
				if (l.getLanguage().isPresent()) {
					properties.put(LANG, l.getLanguage().get());
				}
				result = graphDb.createNode(properties, label);
				break;
		}
		alreadySeen.put(v.stringValue(), result);
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
