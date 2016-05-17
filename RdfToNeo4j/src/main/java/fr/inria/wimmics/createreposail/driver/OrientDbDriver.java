/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.openrdf.model.Literal;
import org.openrdf.model.Value;

/**
 *
 * @author edemairy
 */
public class OrientDbDriver implements GdbDriver {
	OrientGraphFactory graph;
	@Override
	public void openDb(String dbPath) {
		graph = new OrientGraphFactory(dbPath);	
	}

	@Override
	public void closeDb() {
		graph.close();
	}

	Map<String, Object> alreadySeen = new HashMap<>();
	@Override
	public Object createNode(Value v) {
		OrientGraph g = graph.getTx();
		Object result = null;
		if (alreadySeen.containsKey(v.stringValue())) {
			return alreadySeen.get(v.stringValue());
		}
		switch (RdfToGraph.getKind(v)) {
			case IRI:
			case BNODE: {
				Vertex newVertex = g.addVertex();
				newVertex.property(VALUE, v.stringValue());
				newVertex.property(KIND, RdfToGraph.getKind(v));
				result = newVertex.id();
				break;
			}
			case LITERAL: {
				Literal l = (Literal) v;
				Vertex newVertex = g.addVertex();
				newVertex.property(VALUE, l.getLabel());
				newVertex.property(TYPE, l.getDatatype().toString());
				newVertex.property(KIND, RdfToGraph.getKind(v));
				if (l.getLanguage().isPresent()) {
					newVertex.property(LANG, l.getLanguage().get());
				}
				result = newVertex.id();
				break;
			}
		}
		g.commit();
		alreadySeen.put(v.stringValue(), result);
		return result;
	}

	@Override
	public Object createRelationship(Object source, Object object, String predicate, Map<String, Object> properties) {
		Object result = null;
		OrientGraph g = graph.getTx();
		Vertex vSource = g.vertices(source).next();
		Vertex vObject = g.vertices(object).next();
		ArrayList<Object> p = new ArrayList<>();
		properties.keySet().stream().forEach((key) -> {
			p.add(key);
			p.add(properties.get(key));
		});
		Edge e = vSource.addEdge("rdf_edge", vObject, p.toArray());
		result = e.id();
		g.commit();
		return result;
	}
	
}