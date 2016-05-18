/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and openDb the template in the editor.
 */
package fr.inria.wimmics.createreposail.driver;

import fr.inria.wimmics.createreposail.RdfToGraph;
import static fr.inria.wimmics.createreposail.RdfToGraph.BNODE;
import static fr.inria.wimmics.createreposail.RdfToGraph.IRI;
import static fr.inria.wimmics.createreposail.RdfToGraph.KIND;
import static fr.inria.wimmics.createreposail.RdfToGraph.LANG;
import static fr.inria.wimmics.createreposail.RdfToGraph.LITERAL;
import static fr.inria.wimmics.createreposail.RdfToGraph.TYPE;
import static fr.inria.wimmics.createreposail.RdfToGraph.VALUE;
import java.io.File;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.openrdf.model.Literal;
import org.openrdf.model.Value;

/**
 *
 * @author edemairy
 */
public class Neo4jDriver extends GdbDriver {

	BatchInserter inserter;
	private static final Logger LOGGER = Logger.getLogger(Neo4jDriver.class.getName());

	@Override
	public void openDb(String dbPath) {
		try {
			File dbDir = new File(dbPath);
			if (getWipeOnOpen()) {
				Files.walk(Paths.get(dbPath), FileVisitOption.FOLLOW_LINKS)
					.sorted(Comparator.reverseOrder())
					.map(Path::toFile)
					.peek(path -> LOGGER.log(Level.INFO, "removing: {0}", path))
					.forEach(File::delete);
			}
			inserter = BatchInserters.inserter(dbDir);
		} catch (Exception e) {
			LOGGER.severe(e.toString());
			e.printStackTrace();
		}
	}

	@Override
	public void closeDb() {
		inserter.shutdown();
	}

	protected boolean nodeEquals(Node endNode, Value object) {
		boolean result = true;
		result &= endNode.getProperty(KIND).equals(RdfToGraph.getKind(object));
		if (result) {
			switch (RdfToGraph.getKind(object)) {
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
	Map<String, Long> alreadySeen = new HashMap<>();

	String nodeId(Value v) {
		StringBuilder result = new StringBuilder();
		String kind = RdfToGraph.getKind(v);
		switch (kind) {
			case IRI:
			case BNODE:
				result.append("label=" + v.stringValue() + ";");
				result.append("value=" + v.stringValue() + ";");
				result.append("kind=" + kind);
				break;
			case LITERAL:
				Literal l = (Literal) v;
				result.append("label=" + l.getLabel() + ";");
				result.append("value=" + l.getLabel() + ";");
				result.append("type=" + l.getDatatype().toString() + ";");
				result.append("kind=" + kind);
				if (l.getLanguage().isPresent()) {
					result.append("lang=" + l.getLanguage().get() + ";");
				}
				break;
		}
		return result.toString();
	}

	/**
	 * Returns a new node if v does not exist yet.
	 *
	 * @param v
	 * @param context
	 * @return
	 */
	@Override
	public Object createNode(Value v
	) {
		Long result = null;
		String nodeId = nodeId(v);
		if (alreadySeen.containsKey(nodeId)) {
			return alreadySeen.get(nodeId);
		}
		Label label;
		Map<String, Object> properties = new HashMap();
		switch (RdfToGraph.getKind(v)) {
			case IRI:
			case BNODE:
				label = DynamicLabel.label(v.stringValue());
				properties.put(VALUE, v.stringValue());
				properties.put(KIND, RdfToGraph.getKind(v));
				result = inserter.createNode(properties, label);
				break;
			case LITERAL:
				Literal l = (Literal) v;
				label = DynamicLabel.label(l.getLabel());
				properties.put(VALUE, l.getLabel());
				properties.put(TYPE, l.getDatatype().toString());
				properties.put(KIND, RdfToGraph.getKind(v));
				if (l.getLanguage().isPresent()) {
					properties.put(LANG, l.getLanguage().get());
				}
				result = inserter.createNode(properties, label);
				break;
		}
		alreadySeen.put(nodeId, result);
		return result;
	}

	@Override
	public Object createRelationship(Object source, Object object, String predicate, Map<String, Object> properties
	) {
		return inserter.createRelationship((Long) source, (Long) object, DynamicRelationshipType.withName(predicate), properties);
	}
}
