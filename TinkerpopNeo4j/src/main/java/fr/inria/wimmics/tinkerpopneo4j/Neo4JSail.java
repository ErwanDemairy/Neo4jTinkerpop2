/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fr.inria.wimmics.tinkerpopneo4j;

import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;
import com.tinkerpop.blueprints.oupls.sail.GraphSail;
import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.SailException;

/**
 *
 * @author edemairy
 */
public class Neo4JSail implements Runnable {

	private final static Logger LOGGER = Logger.getLogger(Neo4JSail.class.getName());
	private Neo4jGraph graph = null;
	private GraphSail sail;

	public static void main(String[] args) {
		Neo4JSail test = new Neo4JSail();
		test.run();
	}
	private SailRepositoryConnection connection;

	public void openSailConnection() throws SailException, RepositoryException {
		graph = new Neo4jGraph("/tmp/flights");
		sail = new GraphSail(graph);
		sail.initialize();
		connection = new SailRepository(sail).getConnection();
//			connection.setAutoCommit(false);
	}

	public void closeSailConnection() throws SailException, RepositoryException {
		connection.close();
		sail.shutDown();
		graph.shutdown();
	}

	public void importData() throws IOException, RDFParseException, RepositoryException {
		LOGGER.info("import started");
		for (int i = 0; i < 1; i++) {
			connection.add(new File("/Users/edemairy/Downloads/btc-2010-chunk-000"+"_000"+i), null, RDFFormat.NQUADS);
			graph.stopTransaction(TransactionalGraph.Conclusion.SUCCESS);
		}
		LOGGER.info("import ended");
	}

	public void dumpData() throws MalformedQueryException, RepositoryException, QueryEvaluationException {
		LOGGER.info("request of all triples");
		TupleQuery dumpQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, "select * where { ?x ?p ?y}");
		TupleQueryResult result = dumpQuery.evaluate();
		while (result.hasNext()) {
			BindingSet binding = result.next();
			LOGGER.log(Level.INFO, "{0} {1} {2}", new Object[]{binding.getBinding("x").getValue(), binding.getBinding("p").getValue(), binding.getBinding("y").getValue()});
		}
	}

	@Override
	public void run() {
		try {
			openSailConnection();
			importData();
//			dumpData();
			closeSailConnection();
		} catch (SailException ex) {
			Logger.getLogger(Neo4JSail.class.getName()).log(Level.SEVERE, null, ex);
		} catch (RepositoryException ex) {
			Logger.getLogger(Neo4JSail.class.getName()).log(Level.SEVERE, null, ex);
		} catch (IOException ex) {
			Logger.getLogger(Neo4JSail.class.getName()).log(Level.SEVERE, null, ex);
		} catch (RDFParseException ex) {
			Logger.getLogger(Neo4JSail.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
}
