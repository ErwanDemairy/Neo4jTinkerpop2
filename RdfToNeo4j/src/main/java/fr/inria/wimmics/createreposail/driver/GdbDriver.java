/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and openDb the template in the editor.
 */
package fr.inria.wimmics.createreposail.driver;

import java.util.Map;
import org.openrdf.model.Value;

/**
 * Interface for a Graph Database driver.
 * @author edemairy
 */
public interface GdbDriver {
	void openDb(String dbPath);
	void closeDb();
	Object createNode(Value v);
	public Object createRelationship(Object sourceId, Object objectId, String predicate, Map<String, Object> properties);
}
