package com.masterqiao.dasiy;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteRelationship implements Relationship {
	private Logger logger = LoggerFactory.getLogger(RemoteRelationship.class);

	private long id;
	private Transaction tx;

	private Node startNode;
	private Node endNode;

	public RemoteRelationship(Transaction tx, long id, Node startNode, Node endNode) {
		this.tx = tx;
		this.id = id;
		this.startNode = startNode;
		this.endNode = endNode;
	}

	@Override
	public long getId() {
		return id;
	}

	@Override
	public boolean hasProperty(String key) {
		String query = "match ()-[r]->() where id(r)=$id return exists(r." + key + ")";
		Value value = Values.parameters("id", id);
		logger.debug("hasProperty() " + query + " " + value.toString());
		Result ret = tx.run(query, value);
		return ret.single().get(0).isTrue();
	}

	/**
	 *  This method behaves differently with the native api slightly.
	 *  It returns Long, Double and List, instead of returning
	 *  Integer, Float and Array.
	 *  Hope to resolve this gap in the future
	 */
	@Override
	public Object getProperty(String key) {
		String query = "match ()-[r]->() where id(r)=$id return r." + key;
		Value value = Values.parameters("id", id);
		logger.debug("getProperty() " + query + " " + value.toString());
		Result ret = tx.run(query, value);
		Object val = ret.single().get(0).asObject();
		if (val == null) {
			throw new NotFoundException("Property(" + key + ") not found.");
		}
		return val;
	}

	@Override
	public Object getProperty(String key, Object defaultValue) {
		String query = "match ()-[r]->() where id(r)=$id return r." + key;
		Value value = Values.parameters("id", id);
		logger.debug("getProperty() " + query + " " + value.toString());
		Result ret = tx.run(query, value);
		Object val = ret.single().get(0).asObject();
		return val != null ? val : defaultValue;
	}

	@Override
	public void setProperty(String key, Object value) {
		String query = "match ()-[r]->() where id(r)=$id set r." + key + "=$value";
		tx.run(query, Values.parameters("id", id, "value", value));
	}

	@Override
	public Object removeProperty(String key) {
		Object oldObj = getProperty(key, null);
		if (oldObj != null) {
			String query = "match ()-[r]->() where id(r)=$id remove r." + key;
			tx.run(query, Values.parameters("id", id));
		}
		return oldObj;
	}

	@Override
	public Iterable<String> getPropertyKeys() {
		String query = "match ()-[r]->() where id(r)=$id return keys(r)";
		Result ret = tx.run(query, Values.parameters("id", id));
		return ret.single().get("keys(r)").asList(Value::asString);
	}

	@Override
	public Map<String, Object> getProperties(String... keys) {
		Map<String, Object> map = new HashMap<String, Object>();
		if (keys.length == 0) {
			return map;
		}

		StringBuilder strBuilder = new StringBuilder();
		for (String key : keys) {
			strBuilder.append(" r.");
			strBuilder.append(key);
			strBuilder.append(" as ");
			strBuilder.append(key);
			strBuilder.append(',');
		}
		strBuilder.setLength(strBuilder.length() - 1);
		String query = "match ()-[r]->() where id(r)=$id return " + strBuilder.toString();
		Result ret = tx.run(query, Values.parameters("id", id));
		map.putAll(ret.single().asMap());
		return map;
	}

	@Override
	public Map<String, Object> getAllProperties() {
		String query = "match ()-[r]->() where id(r)=$id return r";
		Result ret = tx.run(query, Values.parameters("id", id));
		return ret.single().get(0).asMap();
	}

	@Override
	public void delete() {
		String query = "match ()-[r]->() where id(r)=$id delete r";
		tx.run(query, Values.parameters("id", id));
	}

	@Override
	public Node getStartNode() {
		return startNode;
	}

	@Override
	public Node getEndNode() {
		return endNode;
	}

	@Override
	public Node getOtherNode(Node node) {
		return startNode.getId() == node.getId() ? endNode : startNode;
	}

	@Override
	public Node[] getNodes() {
		return new Node[] { startNode, endNode };
	}

	@Override
	public RelationshipType getType() {
		String query = "match ()-[r]->() where id(r)=$id return type(r)";
		Result ret = tx.run(query, Values.parameters("id", id));
		String str = ret.single().get("type(r)").asString();
		return RelationshipType.withName(str);
	}

	@Override
	public boolean isType(RelationshipType type) {
		return getType().name().equals(type.name());
	}

	@Override
	public String toString() {
		return "(" + startNode.getId() + ")-[" + getType() + "," + id + "]->(" + endNode.getId() + ")";
	}

}
