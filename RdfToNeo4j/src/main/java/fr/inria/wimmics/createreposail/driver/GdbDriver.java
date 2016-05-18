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
public abstract class GdbDriver {
	private boolean wipeOnOpen;

	public abstract void openDb(String dbPath);
	public void setWipeOnOpen(boolean newValue) {
		wipeOnOpen = newValue;
	}
	public boolean getWipeOnOpen() {
		return wipeOnOpen;
	}
	public abstract void closeDb();
	public abstract Object createNode(Value v);
	public abstract Object createRelationship(Object sourceId, Object objectId, String predicate, Map<String, Object> properties);
}
