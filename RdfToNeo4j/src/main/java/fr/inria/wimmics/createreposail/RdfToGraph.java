/*
 * Copyright Inria 2016. 
 */
package fr.inria.wimmics.createreposail;

import fr.inria.wimmics.createreposail.driver.GdbDriver;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

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

import org.openrdf.rio.helpers.NTriplesParserSettings;

/**
 * Tool that: (i) read a RDF file using sesame library; (ii) write the content
 * into a neo4j DB using the neo4j library.
 *
 * @author edemairy
 */
public class RdfToGraph {

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
	protected GdbDriver driver;
	private static final String[] AUTHORIZED_DRIVERS_ARRAY = {"neo4j", "orientdb", "tneo4j", "torientdb"};
	private static final HashSet<String> AUTHORIZED_DRIVERS = new HashSet<>(Arrays.asList(AUTHORIZED_DRIVERS_ARRAY));
	private static final Map<String, String> DRIVER_TO_CLASS;

	static {
		DRIVER_TO_CLASS = new HashMap<>();
		DRIVER_TO_CLASS.put("neo4j", "fr.inria.wimmics.createreposail.driver.Neo4jDriver");
		DRIVER_TO_CLASS.put("orientdb", "fr.inria.wimmics.createreposail.driver.OrientDbDriver");
		DRIVER_TO_CLASS.put("tneo4j", "fr.inria.wimmics.createreposail.driver.TinkerpopNeo4jDriver");
		DRIVER_TO_CLASS.put("torientdb", "fr.inria.wimmics.createreposail.driver.TinkerpopOrientDbDriver");
	}

	RdfToGraph(GdbDriver driver) {
		this.driver = driver;
	}

	public static void main(String[] args) throws FileNotFoundException {
		if (args.length < 2) {
			System.err.println("Usage: rdfToGraph fileName db_path [backend]");
			System.err.println("if the parser cannot guess the format of the input file, NQUADS is used.");
			System.err.println("backend = neo4j | orientdb | tneo4j | torientdb");
			System.err.println("  neo4j     = neo4j directly");
			System.err.println("  orientdb  = orientdb directly");
			System.err.println("  tneo4j    = tinkerpop with neo4j");
			System.err.println("  torientdb = tinkerpop with orientdb");
			System.exit(1);
		}
		String driverName = "neo4j";
		if (args.length >= 3) {
			String driverParam = args[2];
			if (AUTHORIZED_DRIVERS.contains(driverParam)) {
				driverName = driverParam;
			}
		}

		String classDriver = DRIVER_TO_CLASS.get(driverName);
		GdbDriver driver = null;
		try {
			Constructor driverConstructor = Class.forName(classDriver).getConstructor();
			driver = (GdbDriver) driverConstructor.newInstance();
		} catch (ClassNotFoundException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
			Logger.getLogger(RdfToGraph.class.getName()).log(Level.SEVERE, null, ex);
			System.exit(2);
		}
		String rdfFileName = args[0];
		FileInputStream inputStream = new FileInputStream(new File(rdfFileName));
		String dbPath = args[1];

		RdfToGraph converter = new RdfToGraph(driver);

		Optional<RDFFormat> format = Rio.getParserFormatForFileName(rdfFileName);
		if (format.isPresent()) {
			LOGGER.info("Using format: " + format.get());
			converter.convert(inputStream, format.get(), dbPath);
		} else {
			LOGGER.warning("Format of the input file unkown.");
			converter.convert(inputStream, RDFFormat.NQUADS, dbPath);
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
			LOGGER.info("opening the db at " + dbPath);
			driver.openDb(dbPath);
			LOGGER.info("Loading file");
			readFile(rdfStream, format);
			LOGGER.info("Writing graph in db");
			writeModelToNeo4j();
			LOGGER.info("closing DB");
			driver.closeDb();
			LOGGER.info("** end of convert **");
		} catch (IOException ex) {
			Logger.getLogger(RdfToNeo4j.class.getName()).log(Level.SEVERE, null, ex);
		}
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

	final static private int THRESHOLD = 9414890; //Integer.MAX_VALUE;

	public void writeModelToNeo4j() {
		int triples = 0;
		for (Statement statement : model) {
			Resource context = statement.getContext();
			Resource source = statement.getSubject();
			IRI predicat = statement.getPredicate();
			Value object = statement.getObject();

			String contextString = (context == null) ? "" : context.stringValue();

			Object sourceNode = driver.createNode(source);
			Object objectNode = driver.createNode(object);

			Map<String, Object> properties = new HashMap();
			properties.put(CONTEXT, contextString);
			Object relation = driver.createRelationship(sourceNode, objectNode, predicat.stringValue(), properties);
			triples++;
			if (triples > THRESHOLD) {
				break;
			}
		}
		System.out.println(triples + " processed");
	}

	public static String getKind(Value resource) {
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
